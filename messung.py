import adafruit_bme680
import time
import board
import mysql.connector
from datetime import datetime
from sensirion_i2c_driver import LinuxI2cTransceiver, I2cConnection, CrcCalculator
from sensirion_i2c_adapter.i2c_channel import I2cChannel
from sensirion_i2c_scd30.device import Scd30Device

# MySQL connection configuration
mysql_config = {
    'user': 'username',
    'password': 'psk',
    'host': 'localhost',
    'database': 'daten',
}

# Connect to MySQL
conn = mysql.connector.connect(**mysql_config)
cursor = conn.cursor()

# Create table if it does not exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS sensor_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        temperature FLOAT,
        gas INT,
        humidity FLOAT,
        pressure FLOAT,
        altitude FLOAT,
        humidity_scd30 FLOAT,
        temperature_scd30 FLOAT,
        co2_scd30 FLOAT,
        timestamp DATETIME
    )
""")
conn.commit()

# Create BME680 sensor object on the default I2C bus
i2c = board.I2C()  # Uses board.SCL and board.SDA
bme680 = adafruit_bme680.Adafruit_BME680_I2C(i2c)

# Set the location's sea level pressure in hPa
bme680.sea_level_pressure = 1013.25

# Initialize SCD30 sensor
with LinuxI2cTransceiver('/dev/i2c-1') as i2c_transceiver:
    channel = I2cChannel(I2cConnection(i2c_transceiver),
                         slave_address=0x61,
                         crc=CrcCalculator(8, 0x31, 0xff, 0x0))
    sensor = Scd30Device(channel)
    try:
        sensor.stop_periodic_measurement()
        sensor.soft_reset()
        time.sleep(2.0)
    except Exception as e:
        print(f"Error resetting sensor: {e}")

    # Display firmware version
    major, minor = sensor.read_firmware_version()
    print(f"Firmware version - Major: {major}, Minor: {minor}")

    # Start periodic measurement on SCD30
    sensor.start_periodic_measurement(0)

    try:
        while True:
            # Read data from BME680
            temperature = bme680.temperature
            gas = bme680.gas
            humidity = bme680.relative_humidity
            pressure = bme680.pressure
            altitude = bme680.altitude
            timestamp = datetime.now()

            # Read data from SCD30
            co2_concentration, temperature_scd, humidity_scd = sensor.blocking_read_measurement_data()

            # Print sensor data for debugging
            print("\nTemperature: %0.1f C" % temperature)
            print("Gas: %d ohm" % gas)
            print("Humidity: %0.1f %%" % humidity)
            print("Pressure: %0.3f hPa" % pressure)
            print("Altitude = %0.2f meters" % altitude)
            print("Timestamp: %s" % timestamp)
            print(f"CO2 Concentration: {co2_concentration}, Temperature (SCD30): {temperature_scd}, Humidity (SCD30): {humidity_scd}")

            # Insert data into MySQL
            cursor.execute("""
                INSERT INTO sensor_data (temperature, gas, humidity, pressure, altitude, humidity_scd30, temperature_scd30, co2_scd30, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (temperature, gas, humidity, pressure, altitude, humidity_scd, temperature_scd, co2_concentration, timestamp))
            conn.commit()

            # Wait before next reading
            time.sleep(10)

    except KeyboardInterrupt:
        print("Interrupted by user")

    finally:
        # Close MySQL connection and reset SCD30 on exit
        conn.close()
        sensor.soft_reset()
