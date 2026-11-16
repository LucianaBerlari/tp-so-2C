#include "bloqueos.h"
#include "conexion_placa.h"
#include "conexion_storage.h"
#include "util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <commons/log.h>
#include <commons/string.h>

#include <utils/protocolo.h>
#include <utils/util.h>

static t_cola_bloqueo *cola_page_fault;
static t_cola_bloqueo *servicio_loader;
static t_cola_bloqueo *servicio_labeler;
static t_cola_bloqueo *servicio_logger;

static t_cola_bloqueo *checkpoint_save;
static t_cola_bloqueo *checkpoint_load;
static t_cola_bloqueo *checkpoint_delete;

// Para deslockear cuando se desconecta un Core (lo pueden usar varios hilos a la vez -> mutex)
static int socket_placa_general;
static pthread_mutex_t mutex_socket_placa_general = PTHREAD_MUTEX_INITIALIZER;

// Logger del servicio Logger: archivo distinto al log del modulo
static t_log *logger_reportes;

static bool atender_page_fault(int socket_placa, int socket_storage, t_peticion_bloqueo *peticion);
static bool atender_loader(int socket_placa, int socket_storage, t_peticion_bloqueo *peticion);
static bool atender_labeler(int socket_placa, int socket_storage, t_peticion_bloqueo *peticion);
static bool atender_logger(int socket_placa, int socket_storage, t_peticion_bloqueo *peticion);
static bool atender_checkpoint_save(int socket_placa, int socket_storage, t_peticion_bloqueo *peticion);
static bool atender_checkpoint_load(int socket_placa, int socket_storage, t_peticion_bloqueo *peticion);
static bool atender_checkpoint_delete(int socket_placa, int socket_storage, t_peticion_bloqueo *peticion);

static void *hilo_cola_bloqueo(void *arg);

// ---------------- INICIALIZACION ----------------

static t_cola_bloqueo *crear_cola_bloqueo(char *nombre, bool es_servicio, bool es_checkpoint, t_atencion atender)
{
    t_cola_bloqueo *cola = malloc(sizeof(t_cola_bloqueo));

    cola->nombre = nombre;
    cola->es_servicio = es_servicio;
    cola->es_checkpoint = es_checkpoint;
    cola->cola = list_create();
    pthread_mutex_init(&cola->mutex, NULL);
    sem_init(&cola->sem_pendientes, 0, 0);
    cola->socket_placa = conectar_placa_dedicada(nombre);

    cola->socket_storage = -1;
    if (es_checkpoint)
    {
        cola->socket_storage = conectar_storage_dedicada(nombre);
    }
    cola->atender = atender;

    pthread_t hilo;
    pthread_create(&hilo, NULL, hilo_cola_bloqueo, cola);
    pthread_detach(hilo);

    return cola;
}

void iniciar_bloqueos(void)
{
    logger_reportes = log_create(path_reportes, "REPORTES", false, LOG_LEVEL_INFO);
    if (logger_reportes == NULL)
    {
        log_error(logger, "No se pudo crear el archivo de reportes: %s", path_reportes);
        exit(EXIT_FAILURE);
    }

    socket_placa_general = conectar_placa_dedicada("DESLOCKEOS");

    cola_page_fault = crear_cola_bloqueo("PAGE_FAULT", false, false, atender_page_fault);
    servicio_loader = crear_cola_bloqueo("LOADER", true, false, atender_loader);
    servicio_labeler = crear_cola_bloqueo("LABELER", true, false, atender_labeler);
    servicio_logger = crear_cola_bloqueo("LOGGER", true, false, atender_logger);

    checkpoint_save = crear_cola_bloqueo("CHECKPOINT_SAVE", false, true, atender_checkpoint_save);
    checkpoint_load = crear_cola_bloqueo("CHECKPOINT_LOAD", false, true, atender_checkpoint_load);
    checkpoint_delete = crear_cola_bloqueo("CHECKPOINT_DELETE", false, true, atender_checkpoint_delete);
}

// ---------------- ENTRADA A BLOCK ----------------

bool es_motivo_de_bloqueo(t_motivo_desalojo motivo)
{
    return motivo == PAGE_FAULT || motivo == SYSCALL_LOAD_BATCH ||
           motivo == SYSCALL_LABEL || motivo == SYSCALL_REPORT ||
           motivo == SYSCALL_SAVE_CHECKPOINT || motivo == SYSCALL_LOAD_CHECKPOINT || motivo == SYSCALL_DELETE_CHECKPOINT;
}

static t_cola_bloqueo *cola_segun_motivo(t_motivo_desalojo motivo)
{
    switch (motivo)
    {
    case PAGE_FAULT:
        return cola_page_fault;
    case SYSCALL_LOAD_BATCH:
        return servicio_loader;
    case SYSCALL_LABEL:
        return servicio_labeler;
    case SYSCALL_REPORT:
        return servicio_logger;
    case SYSCALL_SAVE_CHECKPOINT:
        return checkpoint_save;
    case SYSCALL_LOAD_CHECKPOINT:
        return checkpoint_load;
    case SYSCALL_DELETE_CHECKPOINT:
        return checkpoint_delete;
    default:
        return NULL;
    }
}

static void destruir_peticion(t_peticion_bloqueo *peticion)
{
    free(peticion->nombre_archivo);
    if (peticion->direcciones != NULL)
        list_destroy_and_destroy_elements(peticion->direcciones, free);
    free(peticion);
}

// valores = [jid][registros][motivo] + extras:
//   PAGE_FAULT:          [nro_pagina]
//   syscalls de memoria: [nombre ("" si no aplica)][tamanio][df_1]...[df_n]
static t_peticion_bloqueo *parsear_peticion(t_job *job, t_motivo_desalojo motivo, t_list *valores)
{
    t_peticion_bloqueo *peticion = calloc(1, sizeof(t_peticion_bloqueo));
    peticion->job = job;
    peticion->motivo = motivo;

    if (motivo == PAGE_FAULT)
    {
        if (list_size(valores) < 4)
        {
            log_error(logger, "## (%u) - Desalojo por PAGE_FAULT sin numero de pagina", job->jid);
            free(peticion);
            return NULL;
        }
        peticion->nro_pagina = *(uint32_t *)list_get(valores, 3);
        return peticion;
    }

    if (motivo == SYSCALL_DELETE_CHECKPOINT)
    {
        if (list_size(valores) < 4)
        {
            log_error(logger,"(%u) - DELETE_CHECKPOINT sin nombre de checkpoint", job->jid);

            free(peticion);
            return NULL;
        }

        char *nombre = list_get(valores, 3);

        if (nombre == NULL || strlen(nombre) == 0)
        {
            log_error(logger, "(%u) - DELETE_CHECKPOINT con nombre invalido", job->jid);
            free(peticion);
            return NULL;
        }

        peticion->nombre_archivo = strdup(nombre);

        return peticion;
    }

    if (list_size(valores) < 6)
    {
        log_error(logger, "## (%u) - Desalojo por %s con formato invalido", job->jid, motivo_de_desalojo_a_string(motivo));
        free(peticion);
        return NULL;
    }

    char *nombre = list_get(valores, 3);
    peticion->nombre_archivo = strlen(nombre) > 0 ? strdup(nombre) : NULL;
    peticion->tamanio = *(uint32_t *)list_get(valores, 4);
    peticion->direcciones = list_create();

    // Del indice 5 en adelante son las direcciones fisicas (una por pagina)
    for (int i = 5; i < list_size(valores); i++)
    {
        t_direccion_fisica *df = malloc(sizeof(t_direccion_fisica));
        memcpy(df, list_get(valores, i), sizeof(t_direccion_fisica));
        list_add(peticion->direcciones, df);
    }

    return peticion;
}

void bloquear_job(t_job *job, t_motivo_desalojo motivo, t_list *valores)
{
    t_cola_bloqueo *destino = cola_segun_motivo(motivo);
    t_peticion_bloqueo *peticion = parsear_peticion(job, motivo, valores);

    if (destino == NULL || peticion == NULL)
    {
        // Sin datos validos no se puede atender: lo devolvemos a READY para no perder el Job
        log_error(logger, "## (%u) - No se pudo bloquear el Job, vuelve a READY", job->jid);
        pasar_a_ready(job);
        return;
    }

    if (motivo == PAGE_FAULT)
        job->cant_page_faults++;
    else
        registrar_syscall(job->jid, motivo_de_desalojo_a_string(motivo));

    // Termino su rafaga de CPU: se recalcula la estimacion (HRRN)
    actualizar_estimacion_prox_rafaga(job);

    pasar_a_block(job);

    pthread_mutex_lock(&destino->mutex);
    list_add(destino->cola, peticion);
    pthread_mutex_unlock(&destino->mutex);

    sem_post(&destino->sem_pendientes); // despierta al hilo de esa cola
}

// ---------------- HILO DE CADA COLA (FIFO, de a uno) ----------------

static void *hilo_cola_bloqueo(void *arg)
{
    t_cola_bloqueo *cola = arg;

    while (1)
    {
        sem_wait(&cola->sem_pendientes);

        pthread_mutex_lock(&cola->mutex);
        t_peticion_bloqueo *peticion = list_remove(cola->cola, 0);
        pthread_mutex_unlock(&cola->mutex);

        t_job *job = peticion->job;

        if (cola->es_servicio)
            log_info(logger, "## (%u) inicia %s", job->jid, cola->nombre);

        bool ok = cola->atender(cola->socket_placa, cola->socket_storage, peticion);

        // Las syscalls de memoria llegan con paginas lockeadas por el Core:
        // el enunciado pide deslockearlas ANTES de pasar a READY

        // DELETE no necesita deslockear páginas, porque no recibe direcciones de memoria.

        if (cola->es_servicio || cola->es_checkpoint)
        {
            if (strcmp(cola->nombre, "CHECKPOINT_DELETE") != 0)
            {
                placa_deslockear_paginas(cola->socket_placa, job->jid);
            }
        }

        if (!ok)
        {
            log_error(logger, "(%u) - Fallo la atencion de %s, se finaliza el Job", job->jid, cola->nombre);
            destruir_peticion(peticion);
            finalizar_job(job, "ERROR", SIN_CORE);
            continue;
        }

        if (cola->es_servicio)
            log_info(logger, "##(%u) finalizó %s y pasa a READY", job->jid, cola->nombre);

        if (cola->es_checkpoint) // No lo junte con el otro, porque el otro es un log obligatorio para servicios
        {
            log_info(logger, "Checkpoint %s finalizado - Job %u pasa a READY", cola->nombre, job->jid);
        }

        pasar_a_ready(job);
        destruir_peticion(peticion);
    }
    return NULL;
}

// ---------------- ATENCIONES ----------------

static bool atender_page_fault(int socket_placa, int socket_storage, t_peticion_bloqueo *peticion)
{
    return placa_cargar_pagina(socket_placa, peticion->job->jid, peticion->nro_pagina);
}

static bool atender_loader(int socket_placa, int socket_storage, t_peticion_bloqueo *peticion)
{
    uint32_t jid = peticion->job->jid;

    if (peticion->nombre_archivo == NULL)
    {
        log_error(logger, "## (%u) - LOAD_BATCH sin nombre de archivo", jid);
        return false;
    }

    char *path = string_ends_with(path_datos, "/")
                     ? string_from_format("%s%s", path_datos, peticion->nombre_archivo)
                     : string_from_format("%s/%s", path_datos, peticion->nombre_archivo);

    FILE *archivo = fopen(path, "rb");
    if (archivo == NULL)
    {
        log_error(logger, "## (%u) - Loader: no se pudo abrir %s", jid, path);
        free(path);
        return false;
    }

    // calloc: si el archivo es mas corto que lo pedido, el resto queda en 0
    void *buffer = calloc(1, peticion->tamanio);
    size_t leidos = fread(buffer, 1, peticion->tamanio, archivo);
    fclose(archivo);

    if (leidos < peticion->tamanio)
        log_warning(logger, "## (%u) - Loader: %s tiene %zu bytes, se pidieron %u", jid, path, leidos, peticion->tamanio);
    free(path);

    usleep(retardo_loader * 1000); // RETARDO_LOADER esta en milisegundos

    bool ok = placa_escribir_memoria_job(socket_placa, jid, peticion->direcciones, buffer);
    free(buffer);
    return ok;
}

static bool atender_labeler(int socket_placa, int socket_storage, t_peticion_bloqueo *peticion)
{
    uint32_t jid = peticion->job->jid;
    uint32_t cantidad = peticion->tamanio;

    char *ingresado = calloc(1, cantidad + 1);
    uint32_t cargados = 0;

    log_info(logger, "## (%u) - Ingrese %u caracteres:", jid, cantidad);

    // Acumula lineas hasta tener la cantidad pedida; el '\n' no cuenta y lo que sobra se descarta
    while (cargados < cantidad)
    {
        char *linea = NULL;
        size_t capacidad = 0;
        ssize_t largo = getline(&linea, &capacidad, stdin);

        if (largo == -1)
        {
            free(linea);
            free(ingresado);
            log_error(logger, "## (%u) - Labeler: se cerro la entrada estandar", jid);
            return false;
        }

        if (largo > 0 && linea[largo - 1] == '\n')
            linea[--largo] = '\0';

        uint32_t faltan = cantidad - cargados;
        uint32_t a_copiar = (uint32_t)largo < faltan ? (uint32_t)largo : faltan;
        memcpy(ingresado + cargados, linea, a_copiar);
        cargados += a_copiar;
        free(linea);

        if (cargados < cantidad)
            log_info(logger, "## (%u) - Faltan %u caracteres:", jid, cantidad - cargados);
    }

    bool ok = placa_escribir_memoria_job(socket_placa, jid, peticion->direcciones, ingresado);
    free(ingresado);
    return ok;
}

static bool atender_logger(int socket_placa, int socket_storage, t_peticion_bloqueo *peticion)
{
    uint32_t jid = peticion->job->jid;
    unsigned char *buffer = malloc(peticion->tamanio);

    if (!placa_leer_memoria_job(socket_placa, jid, peticion->direcciones, buffer))
    {
        free(buffer);
        return false;
    }

    // Cada byte -> 2 caracteres hexa. Ej: "AB" -> "4142"
    char *hexa = string_new();
    for (uint32_t i = 0; i < peticion->tamanio; i++)
        string_append_with_format(&hexa, "%02X", buffer[i]);

    log_info(logger_reportes, "## (%u) - %s", jid, hexa);

    free(hexa);
    free(buffer);
    return true;
}

static bool atender_checkpoint_save(int socket_placa, int socket_storage, t_peticion_bloqueo *peticion)
{
    void *contenido = malloc(peticion->tamanio);

    if (contenido == NULL)
    {
        log_error(logger, "Error al reservar memoria para el checkpoint '%s' - JID: %u", peticion->nombre_archivo, peticion->job->jid);
        return false;
    }

    bool respuesta_lectura = placa_leer_memoria_job(socket_placa, peticion->job->jid, peticion->direcciones, contenido);

    if (!respuesta_lectura)
    {
        log_error(logger, "Error al leer la memoria del Job %u desde Placa para guardar el checkpoint '%s'",
                  peticion->job->jid, peticion->nombre_archivo);

        free(contenido);
        return false;
    }

    int respuesta_check = solicitud_guardar_checkpoint_storage(socket_storage, peticion->nombre_archivo, contenido, peticion->tamanio);

    free(contenido);

    return respuesta_check;
}

static bool atender_checkpoint_load(int socket_placa, int socket_storage, t_peticion_bloqueo *peticion)
{
    void *contenido_respuesta = solicitud_recuperar_checkpoint_storage(socket_storage, peticion->nombre_archivo, peticion->tamanio);

    if (contenido_respuesta == NULL)
    {

        log_error(logger, "Error al recuperar el checkpoint '%s' desde Storage - JID: %u", peticion->nombre_archivo, peticion->job->jid);
        return false;
    }

    bool respuesta = placa_escribir_memoria_job(socket_placa, peticion->job->jid, peticion->direcciones, contenido_respuesta);
    free(contenido_respuesta);

    if (!respuesta)
    {
        log_error(logger, "Error al escribir el checkpoint '%s' en la memoria de Placa - JID: %u", peticion->nombre_archivo, peticion->job->jid);
    }
    else
    {
        log_info(logger, "Checkpoint '%s' cargado correctamente en Placa - JID: %u - Tamaño: %u bytes", peticion->nombre_archivo, peticion->job->jid, peticion->tamanio);
    }

    return respuesta;
}

static bool atender_checkpoint_delete(int socket_placa, int socket_storage, t_peticion_bloqueo *peticion)
{
    return solicitud_eliminacion_checkpoint_storage(socket_storage, peticion->nombre_archivo);
}
// ---------------- DESCONEXION DE CORE ----------------

void deslockear_paginas_de_job(uint32_t jid)
{
    pthread_mutex_lock(&mutex_socket_placa_general);
    placa_deslockear_paginas(socket_placa_general, jid);
    pthread_mutex_unlock(&mutex_socket_placa_general);
}