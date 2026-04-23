# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. **DONE**
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente. **DONE**
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.**DONE**
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway. **DONE**

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.


## Coordinación entre instancias de Sum y Aggregation

### Partición de datos hacia los Aggregators
Cada Aggregator tiene una partición de todas las frutas y se dedica siempre a acumular solo el conteo total de las frutas incluidas en su propia partición. 
Cada instancia de Sum (Summers!) envía los datos (fruta, cantidad) a los Aggregators usando un hashing consistente (tal que todos los Summers hagan el mismo hashing) para determinar a qué Aggregator le corresponde cada fruta. Esto garantiza que todas las sumas parciales de una misma fruta lleguen siempre al mismo Aggregator, evitando procesamiento redundante y permitiendo que cada Aggregator trabaje de forma independiente.

### Coordinación del EOF entre instancias de Sum

El problema principal de la implementación inicial es que el Gateway solo le avisa el fin de ingesta de datos a un solo Summer (el único que recibe el mensaje EOF), pero todas las instancias estuvieron procesando datos en paralelo. La solución implementada es una barrera centralizada con líder:

1. La instancia de Sum que recibe el EOF del Gateway actúa como líder para ese cliente. El mensaje del Gateway incluye la cantidad total de registros enviados (`N`). De esta manera, el Summer líder puede saber cuantos mensajes totales deben procesar en total todos los Summers.
2. El Summer líder hace broadcast del EOF a TODAS las instancias de Sum via un exchange directo, incluyendo el valor `N` y su propio ID, para indicar que él es el líder.
3. Cada Summer, al recibir ese broadcast, manda al líder todos los acumulados, por fruta, hacia los Aggregators y le reporta cuántos mensajes procesó.
4. A medida que llegan mensajes de datos después del broadcast (que puede pasar por reordenamiento en la cola o debido a un mensaje habiendo llegado más tarde), cada Summer los reenvía nuevamente a los Aggregators y le reporta +1 al líder.
5. El líder acumula los conteos reportados. Cuando la suma total es igual a `N`, es decir, se procesaron todos los mensajes que el gateway había enviado a Summers; en ese momento el líder sabe que todos los datos ya fueron enviados a los Aggregators y emite el EOF hacia todas las instancias de Aggregation con ese client_id. Ahí se "levanta" la barrera centralizada para ese cliente.
6. Cada Aggregator, al recibir su EOF, cierra la ingesta de ese cliente, calcula su top parcial y lo publica en la cola de Join. 
7. En este sistema hay solo una instancia de Joiner que acumula un top parcial por Aggregator, los fusiona, calcula el top global final y lo envía al Gateway, finalizando el pipeline de datos.

## Escalabilidad respecto a clientes y cantidad de controles
En esta implementación, el sistema escala en dos dimensiones: por cantidad de clientes concurrentes y por cantidad de réplicas de controles (Summers y Aggregators).

### Escalabilidad respecto a clientes

Cada cliente se conecta a un único proceso de Gateway, pero dentro de ese Gateway se crea un MessageHandler dedicado a cada flujo de mensajes único de cada cliente. Cada mensaje del MessaggeHandler viaja etiquetado con un client_id hacia los Summers.
Más adelante en la cadena de procesamiento, ese client_id se mantiene en todos los mensajes internos de cada worker (Summers, Aggregators y Joiners) y cada worker también guarda los datos separados por client_id por lo que el estado de cada cliente queda aislado del resto.
De esta forma múltiples clientes pueden estar siendo procesados al mismo tiempo sin mezclar resultados: cada etapa mantiene estructuras separadas por cliente y cada EOF coordina únicamente su propio flujo.

### Escalabilidad respecto a cantidad de controles

Al aumentar SUM_AMOUNT, hay más instancias de Summer consumiendo de la cola de entrada, repartiendo la carga de ingesta/procesamiento inicial. La cantidad de nodos Summer se puede escalar infinitamente. Esto funciona porque los Summers desencolan de un mismo lugar y Rabbit hace round-robin para ver a quien le toca cada mensaje de los Summers disponibles. 
Al aumentar AGGREGATION_AMOUNT, se incrementa el paralelismo en la etapa de Aggregation: cada fruta se enruta por un hash determinístico a una única partición. Disclaimer, esto permite que cada aggregator ejecute la consulta sobre su partición de frutas,pero implica que los Aggregators no se pueden escalar más allá de la cantidad de frutas distintas. Cómo máximo, en base a la arquitectura que se desarrolló, se puede tener la misma cantidad de Aggregators que de frutas distintas, de forma que cada aggregator procese una fruta en particular. Se podría extender el sistema para escalar infinitamente de forma horizontal, pero para el caso de uso normal de este trabajo, la implementación actual sirve y sobra. 
Los joiners no se esclan, hay uno solo en todo el sistema, tal como se indica en el esquema de robustez. 
