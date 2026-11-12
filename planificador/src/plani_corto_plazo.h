#ifndef PLANI_CORTO_PLAZO_H_
#define PLANI_CORTO_PLAZO_H_

#include <stdlib.h>

#include "planificador.h"

void *planificador_corto_plazo(void *arg);

void despachar_proceso();

void *controlar_quantum(void* arg);
void iniciar_quantum(uint32_t jid, uint32_t id_core, uint32_t nro_despacho);
double calcular_response_ratio(t_job *job);
t_job *mayor_response_ratio();

typedef struct {
    u_int32_t jid;
    u_int32_t id_core;      // se busca al despertar (el Core se puede haber desconectado)
    u_int32_t nro_despacho; // identifica ESTA rafaga: si el Job se bloqueo y volvio, es otra
    u_int32_t quantum;
} t_args_quantum;

#endif