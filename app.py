import paho.mqtt.client as mqtt
import os
import json
import time

# --- 1. Configuración leída desde las variables de Railway ---

# Lee las variables de entorno de Railway (o usa valores por defecto si no existen)
MQTT_BROKER = os.environ.get("MQTT_BROKER")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883)) # El puerto debe ser un número
MQTT_TOPIC = os.environ.get("MQTT_TOPIC")

# --- 2. Funciones Callback de MQTT ---

# Esta función se llama cuando el script se conecta al broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"¡Conectado exitosamente al broker en {MQTT_BROKER}:{MQTT_PORT}!")
        # Una vez conectado, nos suscribimos al topic
        client.subscribe(MQTT_TOPIC)
        print(f"Suscrito al topic: '{MQTT_TOPIC}'")
    else:
        print(f"Error al conectar, código de retorno: {rc}")

# Esta función se llama CADA VEZ que llega un mensaje al topic
def on_message(client, userdata, msg):
    print("--- MENSAJE RECIBIDO ---")
    try:
        # 1. Decodificar el mensaje (viene en bytes) a un string
        payload_str = msg.payload.decode("utf-8")
        print(f"Topic: {msg.topic}")
        print(f"Payload (Raw): {payload_str}")

        # 2. Parsear el string (que es JSON) a un diccionario de Python
        data = json.loads(payload_str)
        
        # 3. ¡Aquí haces lo que quieras con los datos!
        # Por ahora, solo lo imprimimos bonito en los logs de Railway
        print("Datos Parseados:")
        print(json.dumps(data, indent=2))
        
        # (Futuro: Aquí es donde guardarías 'data' en tu base de datos)
        # db.collection.insert_one(data)

    except json.JSONDecodeError:
        print(f"Error: El mensaje recibido no es un JSON válido: {payload_str}")
    except Exception as e:
        print(f"Error procesando el mensaje: {e}")

# --- 3. Script Principal ---
if __name__ == "__main__":
    # Validar que las variables de entorno existan
    if not all([MQTT_BROKER, MQTT_PORT, MQTT_TOPIC]):
        print("Error: Faltan variables de entorno.")
        print("Asegúrate de definir MQTT_BROKER, MQTT_PORT y MQTT_TOPIC en Railway.")
        # Espera un poco para que el usuario pueda leer el error en los logs
        time.sleep(60)
        exit(1) # Termina el script si faltan variables

    # Crear el cliente MQTT
    client = mqtt.Client()
    
    # Asignar las funciones callback
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"Intentando conectar a {MQTT_BROKER}:{MQTT_PORT}...")
    
    try:
        # Conectar al broker (tu ngrok)
        client.connect(MQTT_BROKER, MQTT_PORT, 60) # 60 segundos de keepalive

        # loop_forever() es un bucle bloqueante que mantiene el script
        # vivo y escuchando mensajes. Es lo que quieres en Railway.
        client.loop_forever()
        
    except Exception as e:
        print(f"No se pudo conectar al broker: {e}")
        print("Revisa la dirección de ngrok y las variables en Railway.")
        time.sleep(60)
        exit(1)