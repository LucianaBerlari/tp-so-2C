#include "conexion_storage.h"
#include "planificador.h"
#include "util.h"

#include <utils/conexion.h>
#include <utils/comunicacion.h>
#include <utils/protocolo.h>
#include <utils/util.h>

int conectar_storage_dedicada(char *uso)
{
    char *ip_storage = config_get_string_value(config, "IP_STORAGE");
    char *puerto_storage = config_get_string_value(config, "PUERTO_STORAGE");

    int socket = crear_conexion(ip_storage, puerto_storage);

    if (socket == -1)
    {
        log_error(logger, "No se pudo abrir la conexion dedicada a STORAGE para %s", uso);
        exit(EXIT_FAILURE);
    }

    enviar_handshake(socket, PLANIFICADOR);
    log_debug(logger, "Conexion dedicada a STORAGE para %s - socket: %d", uso, socket);
    return socket;
}

bool solicitud_guardar_checkpoint_storage(int socket_storage, char *nombre_archivo, void *contenido, uint32_t tamanio)
{
    t_paquete *paquete = crear_paquete(GUARDAR_CHECKPOINT);
    agregar_a_paquete(paquete, nombre_archivo, strlen(nombre_archivo) + 1);
    agregar_a_paquete(paquete, contenido, tamanio);
    agregar_a_paquete(paquete, &tamanio, sizeof(uint32_t));

    enviar_paquete(paquete, socket_storage);
    eliminar_paquete(paquete);

    int cod_op = recibir_operacion(socket_storage);

    if (cod_op == RESPUESTA_CHECKPOINT_OK)
    {

        log_info(logger, "Checkpoint '%s' guardado correctamente en Storage - Tamaño: %u bytes", nombre_archivo, tamanio);
    }
    else if (cod_op == RESPUESTA_CHECKPOINT_SIN_ESPACIO)
    {

        log_error(logger, "No hay espacio suficiente en Storage para guardar el checkpoint '%s' - Tamaño: %u bytes", nombre_archivo, tamanio);
        return false;
    }
    else
    {
        log_error(logger, "Error al guardar el checkpoint '%s' - Respuesta inesperada de Storage: %d", nombre_archivo, cod_op);
        return false;
    }

    return true;
}

void *solicitud_recuperar_checkpoint_storage(int socket_storage, char *nombre_archivo, u_int32_t tamanio)
{

    t_paquete *paquete = crear_paquete(CARGAR_CHECKPOINT);
    agregar_a_paquete(paquete, nombre_archivo, strlen(nombre_archivo) + 1);
    agregar_a_paquete(paquete, &tamanio, sizeof(u_int32_t));

    enviar_paquete(paquete, socket_storage);
    eliminar_paquete(paquete);

    int cod_op = recibir_operacion(socket_storage);

    if (cod_op == RESPUESTA_CHECKPOINT_OK)
    {

        t_list *paquete_respuesta = recibir_paquete(socket_storage);
        void *contenido = list_remove(paquete_respuesta, 0);
        list_destroy(paquete_respuesta);

        log_info(logger, "Checkpoint '%s' recuperado correctamente de Storage - Tamaño solicitado: %u bytes", nombre_archivo, tamanio);

        return contenido;
    }
    else if (cod_op == RESPUESTA_CHECKPOINT_INEXISTENTE)
    {

        log_error(logger, "No se pudo recuperar el checkpoint '%s' - El checkpoint no existe en Storage", nombre_archivo);

        return NULL;
    }
    else
    {
        log_error(logger, "Error al recuperar el checkpoint '%s' - Respuesta inesperada de Storage: %d", nombre_archivo, cod_op);
        return NULL;
    }
}

bool solicitud_eliminacion_checkpoint_storage(int socket_storage, char *nombre_archivo)
{

    t_paquete *paquete = crear_paquete(ELIMINAR_CHECKPOINT);
    agregar_a_paquete(paquete, nombre_archivo, strlen(nombre_archivo) + 1);
    enviar_paquete(paquete, socket_storage);
    eliminar_paquete(paquete);

    int cod_op = recibir_operacion(socket_storage);

    if (cod_op == RESPUESTA_CHECKPOINT_OK)
    {

        log_info(logger,
                 "Checkpoint '%s' eliminado correctamente de Storage",
                 nombre_archivo);

        return true;
    }
    else if (cod_op == RESPUESTA_CHECKPOINT_INEXISTENTE)
    {

        log_error(logger,
                  "No se pudo eliminar el checkpoint '%s' - No existe en Storage",
                  nombre_archivo);
        return true; // devuelve true igual xq no se tiene que eliminar. Mirar funcion "hilo_cola_bloqueo2
    }
    else
    {
        log_error(logger, "Error al guardar el checkpoint '%s' - Respuesta inesperada de Storage: %d", nombre_archivo, cod_op);
        return false;
    }
}