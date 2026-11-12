#ifndef CONEXION_STORAGE_H_
#define CONEXION_STORAGE_H_

#include <stdbool.h>
#include <stdint.h>

int conectar_storage_dedicada(char* nombre);

bool solicitud_guardar_checkpoint_storage(int socket_storage,char* nombre_archivo, void* contenido, uint32_t tamanio);
void *solicitud_recuperar_checkpoint_storage(int socket_storage, char *nombre_archivo, uint32_t tamanio);
bool solicitud_eliminacion_checkpoint_storage(int socket_storage, char *nombre_archivo);


#endif