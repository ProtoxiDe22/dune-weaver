"""MQTT utilities and callback management."""
import os
from typing import Dict, Callable
from modules.core.pattern_manager import (
    run_theta_rho_file, stop_execution, pause_execution,
    resume_execution, get_execution_status, THETA_RHO_DIR
)
from modules.serial.serial_manager import send_command, get_serial_status

def create_mqtt_callbacks() -> Dict[str, Callable]:
    """Create and return the MQTT callback registry."""
    return {
        'run_pattern': lambda file_path: run_theta_rho_file(file_path),
        'stop': stop_execution,
        'pause': pause_execution,
        'resume': resume_execution,
        'home': lambda: send_command("HOME"),
        'set_speed': lambda speed: send_command(f"SET_SPEED {speed}")
    }

def get_mqtt_state():
    """Get the current state for MQTT updates."""
    # Get list of pattern files
    patterns = []
    for root, _, filenames in os.walk(THETA_RHO_DIR):
        for file in filenames:
            if file.endswith('.thr'):
                patterns.append(file)
    
    # Get current execution status
    status = get_execution_status()
    is_running = status.get('is_running', False)
    current_file = status.get('current_file', '')
    
    # Get serial status
    serial_status = get_serial_status()
    
    return {
        'is_running': is_running,
        'current_file': current_file,
        'patterns': sorted(patterns),
        'serial': serial_status.get('status', '')
    } 