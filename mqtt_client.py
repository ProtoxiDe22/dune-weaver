import os
import threading
import time
import paho.mqtt.client as mqtt
import json
from typing import Callable, Dict

class MQTTHandler:
    def __init__(self, callback_registry: Dict[str, Callable]):
        # MQTT Configuration from environment variables
        self.broker = os.getenv('MQTT_BROKER', 'localhost')
        self.port = int(os.getenv('MQTT_PORT', '1883'))
        self.username = os.getenv('MQTT_USERNAME')
        self.password = os.getenv('MQTT_PASSWORD')
        self.client_id = os.getenv('MQTT_CLIENT_ID', 'sand_table')
        self.status_topic = os.getenv('MQTT_STATUS_TOPIC', 'sand_table/status')
        self.command_topic = os.getenv('MQTT_COMMAND_TOPIC', 'sand_table/command')
        self.status_interval = int(os.getenv('MQTT_STATUS_INTERVAL', '30'))

        # Initialize MQTT client
        self.client = mqtt.Client(client_id=self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        # Set credentials if provided
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)

        # Store callback registry
        self.callback_registry = callback_registry

        # Threading control
        self.running = False
        self.status_thread = None

        # Home Assistant MQTT Discovery settings
        self.discovery_prefix = os.getenv('MQTT_DISCOVERY_PREFIX', 'homeassistant')
        self.device_name = os.getenv('HA_DEVICE_NAME', 'Sand Table')
        self.device_id = os.getenv('HA_DEVICE_ID', 'sand_table')
        
        # Additional topics for state
        self.current_file_topic = f"sand_table/state/current_file"
        self.running_state_topic = f"sand_table/state/running"
        # self.playlist_select_topic = f"sand_table/playlist/set"
        self.serial_state_topic = f"sand_table/state/serial"
        
        # Additional topics for pattern files
        self.pattern_select_topic = f"sand_table/pattern/set"
        
        # Store current state
        self.current_file = ""
        self.is_running = False
        self.serial_state = ""
        # self.playlists = []
        self.patterns = []

    def setup_ha_discovery(self):
        """Publish Home Assistant MQTT discovery configurations"""
        base_device = {
            "identifiers": [self.device_id],
            "name": self.device_name,
            "model": "Dune Weaver",
            "manufacturer": "DIY"
        }
        
        # Serial State Sensor
        serial_config = {
            "name": f"{self.device_name} Serial state",
            "unique_id": f"{self.device_id}_serial_state",
            "state_topic": self.serial_state_topic,
            "device": base_device,
            "icon": "mdi:machine",
            "entity_category": "diagnostic"
        }
        self._publish_discovery("sensor", "serial_state", serial_config)

        # Running State Sensor
        running_config = {
            "name": f"{self.device_name} State",
            "unique_id": f"{self.device_id}_running_state",
            "state_topic": self.running_state_topic,
            "device": base_device,
            "icon": "mdi:machine",
            "entity_category": "diagnostic"
        }
        self._publish_discovery("binary_sensor", "running_state", running_config)

        # Current File Sensor
        file_config = {
            "name": f"{self.device_name} Current Pattern",
            "unique_id": f"{self.device_id}_current_file",
            "state_topic": self.current_file_topic,
            "device": base_device,
            "icon": "mdi:file-document-outline"
        }
        self._publish_discovery("sensor", "current_file", file_config)

        # # Playlist Select
        # playlist_config = {
        #     "name": f"{self.device_name} Playlist",
        #     "unique_id": f"{self.device_id}_playlist",
        #     "command_topic": self.playlist_select_topic,
        #     "state_topic": f"{self.playlist_select_topic}/state",
        #     "options": self.playlists,
        #     "device": base_device,
        #     "icon": "mdi:playlist-play"
        # }
        # self._publish_discovery("select", "playlist", playlist_config)

        # Pattern Select
        pattern_config = {
            "name": f"{self.device_name} Pattern",
            "unique_id": f"{self.device_id}_pattern",
            "command_topic": self.pattern_select_topic,
            "state_topic": f"{self.pattern_select_topic}/state",
            "options": self.patterns,
            "device": base_device,
            "icon": "mdi:draw"
        }
        self._publish_discovery("select", "pattern", pattern_config)

    def _publish_discovery(self, component: str, config_type: str, config: dict):
        """Helper method to publish HA discovery configs"""
        discovery_topic = f"{self.discovery_prefix}/{component}/{self.device_id}/{config_type}/config"
        self.client.publish(discovery_topic, json.dumps(config), retain=True)

    def update_state(self, is_running: bool = None, current_file: str = None, 
                    playlists: list = None, patterns: list = None, serial=None):
        """Update the state of the sand table and publish to MQTT"""
        if is_running is not None:
            self.is_running = is_running
            self.client.publish(self.running_state_topic, "ON" if is_running else "OFF", retain=True)
        
        if current_file is not None:
            self.current_file = current_file
            self.client.publish(self.current_file_topic, current_file, retain=True)
        
        # if playlists is not None:
        #     self.playlists = playlists
        #     # Republish discovery config with updated playlist options
        #     self.setup_ha_discovery()
        
        if patterns is not None:
            self.patterns = patterns
            # Republish discovery config with updated pattern options
            self.setup_ha_discovery()
        
        if serial is not None:
            self.serial_state = serial
            self.client.publish(self.serial_state_topic, serial, retain=True)

    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected to MQTT broker with result code {rc}")
        # Subscribe to command topics and pattern selection
        client.subscribe([
            (self.command_topic, 0), 
            # (self.playlist_select_topic, 0),
            (self.pattern_select_topic, 0)
        ])
        # Publish discovery configurations
        self.setup_ha_discovery()

    def on_message(self, client, userdata, msg):
        try:
            # if msg.topic == self.playlist_select_topic:
            #     # Handle playlist selection
            #     playlist_name = msg.payload.decode()
            #     if playlist_name in self.playlists:
            #         self.callback_registry['run_playlist'](playlist_name=playlist_name)
            #         self.client.publish(f"{self.playlist_select_topic}/state", playlist_name, retain=True)
            
            if msg.topic == self.pattern_select_topic:
                # Handle pattern selection
                pattern_name = msg.payload.decode()
                if pattern_name in self.patterns:
                    self.callback_registry['run_pattern'](file_path=f"patterns/{pattern_name}")
                    self.client.publish(f"{self.pattern_select_topic}/state", pattern_name, retain=True)
            
            else:
                # Handle other commands as before
                payload = json.loads(msg.payload.decode())
                command = payload.get('command')
                params = payload.get('params', {})

                if command in self.callback_registry:
                    self.callback_registry[command](**params)
                else:
                    print(f"Unknown command received: {command}")

        except json.JSONDecodeError:
            print(f"Invalid JSON payload received: {msg.payload}")
        except Exception as e:
            print(f"Error processing MQTT message: {e}")

    def publish_status(self):
        while self.running:
            try:
                # Create status message
                status = {
                    "status": "running" if self.is_running else "idle",
                    "timestamp": time.time(),
                    "client_id": self.client_id,
                    "current_file": self.current_file
                }
                # Publish status
                self.client.publish(self.status_topic, json.dumps(status))
                
                # Wait for next interval
                time.sleep(self.status_interval)
            except Exception as e:
                print(f"Error publishing status: {e}")
                time.sleep(5)  # Wait before retry

    def start(self):
        try:
            
            # Connect to broker
            self.client.connect(self.broker, self.port)
            
            # # Start MQTT loop in a separate thread
            self.client.loop_start()
            
            # Start status publishing thread
            self.running = True
            self.status_thread = threading.Thread(target=self.publish_status)
            self.status_thread.daemon = True
            self.status_thread.start()
            
            print("MQTT Handler started successfully")
        except Exception as e:
            print(f"Failed to start MQTT Handler: {e}")

    def stop(self):
        self.running = False
        if self.status_thread:
            self.status_thread.join()
        self.client.loop_stop()
        self.client.disconnect() 