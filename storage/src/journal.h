#ifndef JOURNAL_H_
#define JOURNAL_H_

#include <stdint.h>

void iniciar_journal(void);
uint32_t journal_iniciar_asiento(void);
void journal_op_fat(uint32_t id, uint32_t entrada, uint32_t valor);
void journal_op_write(uint32_t id, uint32_t bloque, void *contenido, uint32_t tamanio);
void journal_op_dir(uint32_t id, uint32_t indice, char *nombre, uint32_t tamanio, uint32_t primer_bloque);
void journal_commit(uint32_t id);
void journal_applied(uint32_t id);

#endif