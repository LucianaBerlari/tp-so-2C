#include "journal.h"
#include "config.h" // storage_config y logger
#include <stdio.h>
#include <stdlib.h>
#include <commons/string.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#include <commons/collections/list.h>
#include "filesystem.h"

#define NOMBRE_JOURNAL "journal.txt"

static FILE *journal = NULL;
static uint32_t proximo_id = 1;
static void resolver_asiento(uint32_t id, t_list*operaciones, bool commiteado, bool aplicado){
    if(commiteado && !aplicado){
        // re aplicamos las operaciones
        log_info(logger,  "## Recuperación - Asiento: %u - APLICADO", id);
    }
    else{
        log_info(logger, "## Recuperación - Asiento: %u - DESCARTADO", id);
    }
    list_clean_and_destroy_elements(operaciones,free); // vacio la lista para el proximo
}
// recorre el journal de principio a fin, asiento por asiento
static void recuperar_journal(char*path){
    FILE *archivo = fopen(path, "r");
    if(archivo == NULL)
    return; // No hay journal primera vez asi que no retorno nada

    t_list*operaciones = list_create(); // las op del asiento actual
    bool hay_asiento = false;
    bool commiteado = false;
    bool aplicado = false;
    uint32_t id_actual = 0;

    char*linea = NULL;
    size_t capacidad = 0;
    while(getline(&linea, &capacidad, archivo)!= -1){
        linea[strcspn(linea, "\n")] = '\0'; // saco los salto de linea
        uint32_t id;

        if(sscanf(linea, "RECORD_STARTED: %u", &id)==1){
            // empieza uno nuevo, el anterior ya esta completo 
            if(hay_asiento)
            resolver_asiento(id_actual, operaciones, commiteado, aplicado);
            hay_asiento=true;
            commiteado = false;
            aplicado = false;
            id_actual = id;
        }
        else if(sscanf(linea, "RECORD_COMMITED %u", &id) ==1)
        commiteado = true;
        else if (sscanf(linea, "RECORD_APPLIED %u", &id) == 1)
            aplicado = true;
        else if (strncmp(linea, "OP_", 3) == 0)
            list_add(operaciones, strdup(linea)); // me la guardo por si hay que re-aplicar
    }
    //el ultimo asiento no tiene un started luego de que lo cierre
    if(hay_asiento)
    resolver_asiento(id_actual, operaciones, commiteado, aplicado);
    free(linea);
    list_destroy(operaciones);
    fclose(archivo);
}









void iniciar_journal(void){
        // PATH_STORAGE ya termina en /, igual que cuando se arma la ruta del volumen
        char *path = string_from_format("%s%s", storage_config->PATH_STORAGE, NOMBRE_JOURNAL);
        recuperar_journal(path);

        // a = append, siempre escribe al final y no borrara lo anterior 
        // lo que quedo de una ejecuccion anterior, lo vamos a necesitar para recuperar

        journal = fopen(path, "a");
        if (journal == NULL){
            log_error(logger, "No se puede abrir el journal: %s", path);
            free(path);
            exit(EXIT_FAILURE);
        }
        free(path);
}

uint32_t journal_iniciar_asiento(void){
    uint32_t id = proximo_id ++;
    fprintf(journal, "RECORD_STARTED: %u\n", id);
    log_info(logger, "## Asiento iniciado: %u", id);
    return id;
}

static void registrar (uint32_t id, char*modificacion){
    fprintf(journal, "%s\n", modificacion);
    log_info(logger, "## Asiento: %u - Modificacion: %s", id, modificacion);
}
void journal_op_fat(uint32_t id, uint32_t entrada, uint32_t valor){
    char *mod= string_from_format("OP_FAT: Entrada 0x%08X -> Valor 0x%08X", entrada, valor);
    registrar(id,mod);
    free(mod);
}
// el contenido va en hexa (2 caracteres por bytes), el bloque Completo
// asi al recuperar, se escribe exactamente igual
void journal_op_write(uint32_t id, uint32_t bloque, void*contenido, uint32_t tamanio){
    char *mod = string_from_format("OP_WRITE: Bloque 0x%08X - Contenido: ", bloque);
    uint8_t *bytes = contenido;
    for(uint32_t i=0; i<tamanio; i++)
    string_append_with_format(&mod, "%02X", bytes[i]);
    
    registrar(id, mod);
    free(mod);
}
void journal_op_dir(uint32_t id, uint32_t indice, char*nombre, uint32_t tamanio, uint32_t primer_bloque){
    char *mod = string_from_format("OP_DIR: Directorio %u - Checkpoint \"%s\" - Tamaño %u - Bloque Inicial 0x%08X",indice, nombre, tamanio, primer_bloque);
    registrar(id,mod);
    free(mod);
}

void journal_commit(uint32_t id){
    usleep(storage_config -> RETARDO_COMMIT *1000);
    fprintf(journal, "RECORD_COMMITED %u\n", id);
    fflush(journal);  // del buffer del programa al sistema operativo
    fsync(fileno(journal)); //del sist operativo al disco

    log_info(logger, "## Nueva marca en asiento: %u: - COMMIT", id);
}
// marca el asiento como aplicado. sin fflush/sync 
// si esta marca se pierde, se re-aplica y da lo mismo
void journal_applied(uint32_t id){
    fprintf(journal, "RECORD_APPLIED %u\n", id);
      log_info(logger, "## Nueva marca en asiento: %u: - APPLIED", id);

}