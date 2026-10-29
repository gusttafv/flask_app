import os
import json
import pika
from flask import Flask, request, jsonify

app = Flask(__name__)

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD', 'guest')
QUEUE_NAME = 'test_queue'

def get_rabbitmq_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials
    )
    return pika.BlockingConnection(parameters)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "ok", "message": "Flask app is running"})

@app.route('/send', methods=['POST'])
def send_message():
    """Отправляет сообщение в очередь RabbitMQ."""
    data = request.get_json()
    message = json.dumps(data)  # Превращаем словарь в строку

    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        # Создаем очередь, если её нет (durable=True — переживет перезапуск RabbitMQ)
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        # Отправляем сообщение
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)  # persistent
        )
        connection.close()
        return jsonify({"status": "sent", "message": message}), 200
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route('/status', methods=['GET'])
def get_status():
    """Забирает одно сообщение из очереди (для проверки)."""
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        # Забираем одно сообщение (auto_ack=True — подтверждаем сразу)
        method_frame, header_frame, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=True)
        connection.close()
        if method_frame:
            return jsonify({"status": "received", "message": body.decode()}), 200
        else:
            return jsonify({"status": "empty", "message": "No messages in queue"}), 200
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
