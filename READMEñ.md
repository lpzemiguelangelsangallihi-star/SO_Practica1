# Práctica 1 – Hilos y sincronización (Sistemas Operativos)

Programa en Python que procesa 24 documentos con 4 trabajadores (hilos). Usa `threading`, `queue.Queue` y `Lock`.
Se ejecuta con `python3 practica1_hilos.py`. Las capturas están en la carpeta `capturas/`.

## Cómo funciona
- Los 24 documentos (id y tamaño de 1 a 5) se guardan en una `queue.Queue`.
- Cuatro hilos (`Trabajador-1` a `Trabajador-4`) sacan documentos con `get_nowait()` hasta que la cola se vacía (capturan `queue.Empty`).
- Cada documento se "procesa" con `time.sleep(tamaño * 0.1)`.
- El conteo de cada trabajador se guarda en un diccionario compartido dentro de `with lock:`.
- El hilo principal espera con `join()` y muestra el total y el resumen.

## Código

```python
"""
import threading
import queue
import time
import random

NUM_DOCUMENTOS = 24
NUM_TRABAJADORES = 4
FACTOR_TIEMPO = 0.1  # segundos de simulación por unidad de tamaño


def cargar_cola():
    """Crea la cola compartida con 24 documentos (id, tamaño simulado)."""
    cola = queue.Queue()
    documentos = [(doc_id, random.randint(1, 5)) for doc_id in range(1, NUM_DOCUMENTOS + 1)]
    for doc in documentos:
        cola.put(doc)
    return cola, documentos


def trabajador(nombre, trabajos, resultados, registro, lock):
    """Retira documentos de la cola hasta que esté vacía y los 'procesa'."""
    while True:
        try:
            # get_nowait() es atómico respecto a otros hilos: cada documento
            # sale de la cola una sola vez. No se usa empty() como condición.
            doc_id, tamano = trabajos.get_nowait()
        except queue.Empty:
            break  # cola vacía: terminación controlada

        time.sleep(tamano * FACTOR_TIEMPO)  # simula el procesamiento

        # Sección crítica: actualización del estado compartido
        with lock:
            resultados[nombre]["docs"] += 1
            resultados[nombre]["tamano"] += tamano
            registro.append((doc_id, nombre))
        trabajos.task_done()


def ejecutar_con_hilos(documentos_cola):
    resultados = {f"Trabajador-{i}": {"docs": 0, "tamano": 0}
                  for i in range(1, NUM_TRABAJADORES + 1)}
    registro = []
    lock = threading.Lock()

    hilos = [threading.Thread(target=trabajador,
                              args=(nombre, documentos_cola, resultados, registro, lock),
                              name=nombre)
             for nombre in resultados]

    inicio = time.perf_counter()
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()
    duracion = time.perf_counter() - inicio
    return resultados, registro, hilos, duracion


def ejecutar_secuencial(documentos):
    inicio = time.perf_counter()
    for _, tamano in documentos:
        time.sleep(tamano * FACTOR_TIEMPO)
    return time.perf_counter() - inicio


def main():
    cola, documentos = cargar_cola()
    print(f"Se cargaron {len(documentos)} documentos en la cola "
          f"(tamaño total simulado: {sum(t for _, t in documentos)})")
    print(f"Iniciando {NUM_TRABAJADORES} trabajadores...\n")

    resultados, registro, hilos, t_hilos = ejecutar_con_hilos(cola)

    # ---- Resumen -------------------------------------------------------
    total = sum(r["docs"] for r in resultados.values())
    print("=" * 52)
    print("RESUMEN POR TRABAJADOR")
    print("=" * 52)
    print(f"{'Trabajador':<14}{'Documentos':>12}{'Tamaño acumulado':>22}")
    for nombre, r in resultados.items():
        print(f"{nombre:<14}{r['docs']:>12}{r['tamano']:>22}")
    print("-" * 52)
    print(f"{'TOTAL':<14}{total:>12}{sum(r['tamano'] for r in resultados.values()):>22}")

    # ---- Asignación detallada de los 24 documentos ---------------------
    print("\nASIGNACIÓN DE DOCUMENTOS (orden de procesamiento)")
    for nombre in resultados:
        asignados = [d for d, n in registro if n == nombre]
        print(f"  {nombre}: {asignados}")

    # ---- Verificaciones -----------------------------------------------
    ids = [d for d, _ in registro]
    v1 = total == NUM_DOCUMENTOS
    v2 = len(ids) == len(set(ids)) == NUM_DOCUMENTOS and set(ids) == set(range(1, 25))
    v3 = all(not h.is_alive() for h in hilos) and cola.empty()
    print("\nVERIFICACIONES")
    print(f"  1. Suma de documentos por trabajador = {NUM_DOCUMENTOS}: {'OK' if v1 else 'FALLO'}")
    print(f"  2. Cada ID procesado exactamente una vez:        {'OK' if v2 else 'FALLO'}")
    print(f"  3. Los 4 hilos terminaron sin bloquearse:        {'OK' if v3 else 'FALLO'}")

    # ---- Reto: comparación secuencial vs hilos -------------------------
    t_sec = ejecutar_secuencial(documentos)
    print("\nRETO: COMPARACIÓN DE TIEMPOS")
    print(f"  Versión secuencial : {t_sec:.2f} s")
    print(f"  Versión 4 hilos    : {t_hilos:.2f} s")
    print(f"  Aceleración        : {t_sec / t_hilos:.2f}x")


if __name__ == "__main__":
    main()
```
 Ejecuciones

**Ejecución 1**

![Ejecución 1](capturas/ejecucion_1.png)

**Ejecución 2**

![Ejecución 2](capturas/ejecucion_2.png)

**Ejecución 3**

![Ejecución 3](capturas/ejecucion_3.png)

**Ejecución 4**

![Ejecución 4](capturas/ejecucion_4.png)

## Resultados (4 ejecuciones)
| Ejecución | Total | T1 / T2 / T3 / T4 |
|---|---|---|
| 1 | 24 | 7 / 5 / 6 / 6 |
| 2 | 24 | 8 / 5 / 6 / 5 |
| 3 | 24 | 6 / 5 / 7 / 6 |
| 4 | 24 | 7 / 5 / 6 / 6 |

Asignación de la ejecución 1:

| Trabajador | Documentos | Cantidad | Tamaño total |
|---|---|---|---|
| Trabajador-1 | 1, 5, 8, 11, 15, 18, 23 | 7 | 19 |
| Trabajador-2 | 2, 7, 13, 20, 24 | 5 | 18 |
| Trabajador-3 | 3, 9, 14, 17, 19, 21 | 6 | 17 |
| Trabajador-4 | 4, 6, 10, 12, 16, 22 | 6 | 16 |

