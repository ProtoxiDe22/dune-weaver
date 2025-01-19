"""Real MQTT handler implementation."""
import os
import threading
import time
import json
from typing import Dict, Callable, List, Optional, Any
import paho.mqtt.client as mqtt
import logging

from .base import BaseMQTTHandler

logger = logging.getLogger(__name__)

class MQTTHandler(BaseMQTTHandler):
    """Real implementation of MQTT handler."""
    
    def __init__(self, callback_registry: Dict[str, Callable]):
        # MQTT Configuration from environment variables
        self.broker = os.getenv('MQTT_BROKER')
        self.port = int(os.getenv('MQTT_PORT', '1883'))
        self.username = os.getenv('MQTT_USERNAME')
        self.password = os.getenv('MQTT_PASSWORD')
        self.client_id = os.getenv('MQTT_CLIENT_ID', 'dune_weaver')
        self.status_topic = os.getenv('MQTT_STATUS_TOPIC', 'dune_weaver/status')
        self.command_topic = os.getenv('MQTT_COMMAND_TOPIC', 'dune_weaver/command')
        self.status_interval = int(os.getenv('MQTT_STATUS_INTERVAL', '30'))

        # Store callback registry
        self.callback_registry = callback_registry

        # Threading control
        self.running = False
        self.status_thread = None

        # Home Assistant MQTT Discovery settings
        self.discovery_prefix = os.getenv('MQTT_DISCOVERY_PREFIX', 'homeassistant')
        self.device_name = os.getenv('HA_DEVICE_NAME', 'Sand Table')
        self.device_id = os.getenv('HA_DEVICE_ID', 'dune_weaver')
        
        # Additional topics for state
        self.current_file_topic = f"{self.device_id}/state/current_file"
        self.running_state_topic = f"{self.device_id}/state/running"
        self.serial_state_topic = f"{self.device_id}/state/serial"
        self.pattern_select_topic = f"{self.device_id}/pattern/set"
        
        # Store current state
        self.current_file = ""
        self.is_running_state = False
        self.serial_state = ""
        self.patterns = []

        # Initialize MQTT client if broker is configured
        if self.broker:
            self.client = mqtt.Client(client_id=self.client_id)
            self.client.on_connect = self.on_connect
            self.client.on_message = self.on_message

            if self.username and self.password:
                self.client.username_pw_set(self.username, self.password)

    def setup_ha_discovery(self):
        """Publish Home Assistant MQTT discovery configurations."""
        if not self.is_enabled:
            return

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

        # Pattern List Sensor
        pattern_list_config = {
            "name": f"{self.device_name} Available Patterns",
            "unique_id": f"{self.device_id}_pattern_list",
            "state_topic": f"{self.device_id}/state/patterns",
            "device": base_device,
            "icon": "mdi:file-multiple",
            "entity_category": "diagnostic"
        }
        self._publish_discovery("sensor", "pattern_list", pattern_list_config)

        # Playlist Active Sensor
        playlist_active_config = {
            "name": f"{self.device_name} Playlist Active",
            "unique_id": f"{self.device_id}_playlist_active",
            "state_topic": "{self.device_id}/state/playlist",
            "value_template": "{{ value_json.active }}",
            "device": base_device,
            "icon": "mdi:playlist-play",
            "entity_category": "diagnostic"
        }
        self._publish_discovery("binary_sensor", "playlist_active", playlist_active_config)

        # Current File Index Sensor
        playlist_index_config = {
            "name": f"{self.device_name} Playlist Index",
            "unique_id": f"{self.device_id}_playlist_index",
            "state_topic": "{self.device_id}/state/playlist",
            "value_template": "{{ value_json.current_index }}",
            "device": base_device,
            "icon": "mdi:playlist-music",
            "entity_category": "diagnostic"
        }
        self._publish_discovery("sensor", "playlist_index", playlist_index_config)

        # Total Files Sensor
        playlist_total_config = {
            "name": f"{self.device_name} Playlist Total Files",
            "unique_id": f"{self.device_id}_playlist_total",
            "state_topic": "{self.device_id}/state/playlist",
            "value_template": "{{ value_json.total_files }}",
            "device": base_device,
            "icon": "mdi:playlist-music",
            "entity_category": "diagnostic"
        }
        self._publish_discovery("sensor", "playlist_total", playlist_total_config)

        # Clearing Pattern Sensor
        playlist_clearing_config = {
            "name": f"{self.device_name} Clearing Pattern",
            "unique_id": f"{self.device_id}_clearing_pattern",
            "state_topic": f"{self.device_id}/state/playlist",
            "value_template": "{{ value_json.is_clearing }}",
            "device": base_device,
            "icon": "mdi:eraser",
            "entity_category": "diagnostic"
        }
        self._publish_discovery("binary_sensor", "clearing_pattern", playlist_clearing_config)

    def _publish_discovery(self, component: str, config_type: str, config: dict):
        """Helper method to publish HA discovery configs."""
        if not self.is_enabled:
            return
            
        discovery_topic = f"{self.discovery_prefix}/{component}/{self.device_id}/{config_type}/config"
        self.client.publish(discovery_topic, json.dumps(config), retain=True)

    def update_state(self, is_running: Optional[bool] = None, 
                    current_file: Optional[str] = None,
                    patterns: Optional[List[str]] = None, 
                    serial: Optional[str] = None,
                    playlist: Optional[Dict[str, Any]] = None) -> None:
        """Update the state of the sand table and publish to MQTT."""
        if not self.is_enabled:
            return

        if is_running is not None:
            self.is_running_state = is_running
            self.client.publish(self.running_state_topic, "ON" if is_running else "OFF", retain=True)
        
        if current_file is not None:
            if current_file:  # Only publish if there's actually a file
                # Extract just the filename without path and normalize it 
                if current_file.startswith('./patterns/'):
                    file_name = current_file[len('./patterns/'):]
                else:
                    file_name = current_file.split("/")[-1].split("\\")[-1]
                
                self.current_file = file_name
                # Update both the current file topic and the pattern select state
                self.client.publish(self.current_file_topic, file_name, retain=True)
                self.client.publish(f"{self.pattern_select_topic}/state", file_name, retain=True)
                logger.info(file_name)
                logger.info(f"{self.pattern_select_topic}/state")
            else:
                # Clear both states when no file is playing
                self.client.publish(self.current_file_topic, "", retain=True)
                self.client.publish(f"{self.pattern_select_topic}/state", "", retain=True)
        
        if patterns is not None:
            # Only proceed if patterns have actually changed
            if set(patterns) != set(self.patterns):
                self.patterns = patterns
                # Publish the current list of patterns
                self.client.publish(f"{self.device_id}/state/patterns", json.dumps(patterns), retain=True)
                # Republish discovery config with updated pattern options
                self.setup_ha_discovery()
        
        if serial is not None:
            self.serial_state = serial
            self.client.publish(self.serial_state_topic, serial, retain=True)
        
        if playlist is not None:
            # Publish playlist information
            self.client.publish(f"{self.device_id}/state/playlist", json.dumps({
                "active": bool(playlist),
                "current_index": playlist.get("current_index", 0) if playlist else None,
                "total_files": len(playlist["files"]) if playlist else 0,
                "is_clearing": playlist.get("is_clearing", False) if playlist else False,
                "files": playlist.get("files", []) if playlist else []
            }), retain=True)

    def on_connect(self, client, userdata, flags, rc):
        """Callback when connected to MQTT broker."""
        logger.info(f"Connected to MQTT broker with result code {rc}")
        # Subscribe to command topics and pattern selection
        client.subscribe([
            (self.command_topic, 0),
            (self.pattern_select_topic, 0)
        ])
        # Publish discovery configurations
        self.setup_ha_discovery()

    def on_message(self, client, userdata, msg):
        """Callback when message is received."""
        
        logger.critical(msg)
        try:
            if msg.topic == self.pattern_select_topic:
                # Handle pattern selection
                pattern_name = msg.payload.decode()
                if pattern_name in self.patterns:
                    self.callback_registry['run_pattern'](file_path=f"patterns/{pattern_name}")
                    self.client.publish(f"{self.pattern_select_topic}/state", pattern_name, retain=True)
            else:
                # Handle other commands
                payload = json.loads(msg.payload.decode())
                command = payload.get('command')
                params = payload.get('params', {})

                if command in self.callback_registry:
                    self.callback_registry[command](**params)
                else:
                    logger.error(f"Unknown command received: {command}")

        except json.JSONDecodeError:
            logger.error(f"Invalid JSON payload received: {msg.payload}")
        except Exception as e:
            logger.error(f"Error processing MQTT message: {e}")

    def publish_status(self):
        """Publish status updates periodically."""
        while self.running:
            try:
                # Create status message
                status = {
                    "status": "running" if self.is_running_state else "idle",
                    "timestamp": time.time(),
                    "client_id": self.client_id,
                    "current_file": self.current_file
                }
                # Publish status
                self.client.publish(self.status_topic, json.dumps(status))
                
                # Wait for next interval
                time.sleep(self.status_interval)
            except Exception as e:
                logger.error(f"Error publishing status: {e}")
                time.sleep(5)  # Wait before retry

    def start(self) -> None:
        """Start the MQTT handler."""
        if not self.is_enabled:
            return
        
        try:
            
            self.client.connect(self.broker, self.port)
            self.client.loop_start()
            # Start status publishing thread
            self.running = True
            self.status_thread = threading.Thread(target=self.publish_status, daemon=True)
            self.status_thread.start()
            
            # Get initial states from modules
            from modules.core.pattern_manager import get_execution_status, get_pattern_files
            from modules.serial.serial_manager import get_serial_status
            
            execution_status = get_execution_status()
            serial_status = get_serial_status()
            patterns = get_pattern_files()

            # Wait a bit for MQTT connection to establish
            time.sleep(1)

            # Publish initial state
            status = {
                "status": "running" if execution_status.get('is_running', False) else "idle",
                "timestamp": time.time(),
                "client_id": self.client_id,
                "current_file": execution_status.get('current_playing_file', '')
            }
            self.client.publish(self.status_topic, json.dumps(status), retain=True)
            self.client.publish(self.running_state_topic, 
                              "ON" if execution_status.get('is_running', False) else "OFF", 
                              retain=True)
            self.client.publish(self.serial_state_topic, 
                              serial_status.get('status', ''), 
                              retain=True)
            
            # Update and publish pattern list
            self.patterns = patterns
            self.client.publish(f"{self.device_id}/state/patterns", json.dumps(patterns), retain=True)
            
            # Get and publish playlist state
            playlist_info = None
            if execution_status.get('current_playlist'):
                playlist_info = {
                    'files': execution_status['current_playlist'],
                    'current_index': execution_status.get('current_playing_index', 0),
                    'is_clearing': execution_status.get('is_clearing', False)
                }
            
            self.client.publish(f"{self.device_id}/state/playlist", json.dumps({
                "active": bool(playlist_info),
                "current_index": playlist_info['current_index'] if playlist_info else None,
                "total_files": len(playlist_info['files']) if playlist_info else 0,
                "is_clearing": playlist_info.get('is_clearing', False) if playlist_info else False,
                "files": playlist_info.get('files', []) if playlist_info else []
            }), retain=True)
            
            self.setup_ha_discovery()
            
            logger.info("MQTT Handler started successfully")
        except Exception as e:
            logger.error(f"Failed to start MQTT Handler: {e}")

    def stop(self) -> None:
        """Stop the MQTT handler."""
        if not self.is_enabled:
            return

        self.running = False
        if self.status_thread:
            self.status_thread.join()
        self.client.loop_stop()
        self.client.disconnect()
        if hasattr(self, 'mqtt_thread'):
            self.mqtt_thread.join(timeout=1)

    @property
    def is_enabled(self) -> bool:
        """Return whether MQTT functionality is enabled."""
        return bool(self.broker) 