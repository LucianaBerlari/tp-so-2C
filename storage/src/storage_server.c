#include "storage_server.h"

#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>
#include <string.h>

#include <utils/comunicacion.h>
#include <utils/util.h>

<<<<<<< HEAD
#include <utils/protocolo.h>
#include "filesystem.h"

/*int iniciar_servidor_storage(void){
    int socket_servidor;
    struct addrinfo  hints, *servinfo;
    // inicializo en 0 los criterios para direccion de red
    memset (&hints, 0, sizeof(hints)); // hint es una variable de tipo estructura, actua como filtro de preferencias
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE; // asigna la ip de la maquina local

    getaddrinfo (NULL, storage_config -> PUERTO_ESCUCHA, &hints, &servinfo); // puerto escucha a una red valida


  // Creamos el socket con los parametros obtenidos
    socket_servidor = socket(servinfo -> ai_family, servinfo -> ai_socktype, servinfo-> ai_protocol); // ai_protocol define capa de transporte del socket
    if (socket_servidor == -1){
        log_error(storage_logger, " Error al crear el socket de storage");
        freeaddrinfo(servinfo);
        return -1;
    }
    // funcion para que no se bloquee el puerto durante un determinado tiempo si cerramos el sistema
    // permitimos reutilizar el puerto inmediatamente al reiniciar
    int optval = 1; // habilitar opcion  (true)
    setsockopt(socket_servidor, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    // enlazamos el socket al puerto configurado
    if(bind(socket_servidor, servinfo -> ai_addr, servinfo-> ai_addrlen)== -1){
        log_error(storage_logger, "Eror en bind al puerto %s", storage_config -> PUERTO_ESCUCHA);
        close(socket_servidor);
        freeaddrinfo(servinfo);
        return -1;
    }

// ponemos al socket en modo de escucha pasiva
if (listen(socket_servidor, SOMAXCONN)== -1){ // somaxconn define un tamaño maximo en la cola de espera de conexiones pendientes
 log_error(storage_logger, "error en listen en el puerto %s", storage_config -> PUERTO_ESCUCHA);
 close(socket_servidor);
 freeaddrinfo(servinfo);
 return -1;
}

 // libero la memoria reservada para getaddrinfo
  freeaddrinfo(servinfo);

  log_info(storage_logger, "servidor listo y escuchando al puerto % s", storage_config -> PUERTO_ESCUCHA);
  return socket_servidor;

}

void esperar_cliente_storage (int server_fd){
    while(1){ // Bucle que se ejecuta siempre
    int * socket_cliente= malloc(sizeof(int));
    *socket_cliente = accept (server_fd, NULL, NULL);

    if (*socket_cliente == -1){
        log_error(storage_logger, "Error al aceptar un nuevo cliente: ");
        free (socket_cliente);
        continue;
    }
    pthread_t hilo_cliente; // identificador del hilo creado
    pthread_create (&hilo_cliente, NULL, atender_cliente_storage, socket_cliente); //crear y arrancar el hilo en 2do plano
    pthread_detach(hilo_cliente); // cuando este hilo termine de atendere al cliente hacer return y liberar memoria

    }
}*/
=======
>>>>>>> e0bfbc2 (Correcion de errores)

void esperar_cliente_storage(int server_fd)
{
    while (1) // Bucle que se ejecuta siempre
    {
        int *socket_cliente = malloc(sizeof(int));
        *socket_cliente = accept(server_fd, NULL, NULL);

        if (*socket_cliente == -1)
        {
            log_error(logger, "Error al aceptar un nuevo cliente: ");
            free(socket_cliente);
            continue;
        }

        pthread_t hilo_cliente;                                                       // identificador del hilo creado
        pthread_create(&hilo_cliente, NULL, atender_cliente_storage, socket_cliente); // crear y arrancar el hilo en 2do plano
        pthread_detach(hilo_cliente);                                                 // cuando este hilo termine de atendere al cliente hacer return y liberar memoria
    }
}

// Responde con un opcode, el planificador solo lee 4 bytes con recibir_operacion
static void responder_codigo(int socket, int codigo)
{
    send(socket, &codigo, sizeof(int), 0);
}

// atencion a los clientes conectados

void *atender_cliente_storage(void *arg)
{
    /*int *puntero_socket = (int *)arg; // convierto el puntero generico a void
    int socket_cliente = *puntero_socket;*/
    // extraemos el valor de dicho socket

    int socket_cliente = *(int *)arg;
    free(arg); // libero la memoria dinamica reservada en malloc

    log_info(logger, "Modulo conectado a Storage");

    t_modulo modulo = recibir_handshake(socket_cliente);

    if (modulo != PLANIFICADOR)
    {
        log_error(logger, "El modulo %i no corresponde como cliente de STORAGE", modulo);
        // list_destroy_and_destroy_elements(paquete_recibido, free);
        close(socket_cliente);
        return NULL;
    }

    char *modulo_s = modulo_a_string(modulo);
    log_info(logger, "Esperando peticiones de %s", modulo_s);

    while (1)
    {
        int codigo_operacion = recibir_operacion(socket_cliente); // Reccibimos el codigo del paquete aca, dentro del switch hay que recibir el paquete

        if (codigo_operacion == -1)
        {
            log_warning(logger, "PLANIFICADOR desconectado");
            break;
        }

        switch (codigo_operacion)
        {
            case MENSAJE:
            {
            // El texto está después del entero de tamaño (payload + 4 bytes)
            /*char* texto = (char*)(paquete->payload + sizeof(uint32_t));
            log_info(logger, "## Mensaje recibido: %s", texto);*/

            int size;
            char *buffer = recibir_buffer(&size, socket_cliente);
            log_info(logger, "Me llego el mensaje: %s", buffer);
            free(buffer);

            // Confirmación simple de vuelta al cliente
            enviar_mensaje("CONFIRMACION_RECIBIDA", socket_cliente);
            break;
            }

            case GUARDAR_CHECKPOINT:        
            {
            t_list *campos = recibir_paquete(socket_cliente);
            char *nombre = list_get(campos, 0);
            void *contenido = list_get(campos, 1);
            uint32_t tamanio = *(uint32_t *)list_get(campos, 2);
            log_info(logger, "## Solicitud Guardar Checkpoint: %s", nombre);
            t_resultado_guardado resultado = guardar_checkpoint(nombre, contenido, tamanio);

                if (resultado == GUARDAR_OK)
                    responder_codigo(socket_cliente, RESPUESTA_CHECKPOINT_OK);
                else
                    responder_codigo(socket_cliente, RESPUESTA_CHECKPOINT_SIN_ESPACIO);

            list_destroy_and_destroy_elements(campos, free);
            break;
            }

            case CARGAR_CHECKPOINT:
            {
            t_list *campos = recibir_paquete(socket_cliente);
            char *nombre = list_get(campos, 0);
            log_info(logger, "## Solicitud Cargar Checkpoint: %s", nombre);
            uint32_t tamanio = 0;
            void *contenido = leer_checkpoint(nombre, &tamanio);

                if (contenido == NULL)
                {
                    responder_codigo(socket_cliente, RESPUESTA_CHECKPOINT_INEXISTENTE);
                }
                else
                {
                    t_paquete *respuesta = crear_paquete(RESPUESTA_CHECKPOINT_OK);
                    agregar_a_paquete(respuesta, contenido, tamanio);
                    enviar_paquete(respuesta, socket_cliente);
                    eliminar_paquete(respuesta);
                    free(contenido);   // leer_checkpoint lo reservó con malloc: lo liberamos nosotros
                }

            list_destroy_and_destroy_elements(campos, free);
            break;
            }

            case ELIMINAR_CHECKPOINT:
            {
            t_list *campos = recibir_paquete(socket_cliente);
            char *nombre = list_get(campos, 0);
            log_info(logger, "## Solicitud Eliminacion Checkpoint: %s", nombre);
            
            if (eliminar_checkpoint(nombre))
                responder_codigo(socket_cliente, RESPUESTA_CHECKPOINT_OK);
            else
                responder_codigo(socket_cliente, RESPUESTA_CHECKPOINT_INEXISTENTE);

            list_destroy_and_destroy_elements(campos, free);
            break;
            }           

        default:
            log_warning(logger, "## Código de operación desconocido: %d", codigo_operacion);
            break;
        }
    }

    close(socket_cliente);
    return NULL;
}