import socket

HOST = "127.0.0.1"
PORT = 30003

with socket.create_connection((HOST, PORT)) as sock:
    file = sock.makefile()
    for line in file:
        parts = line.strip().split(",")

        if len(parts) < 22 or parts[0] != "MSG":
            continue

        icao = parts[4]
        flight = parts[10].strip()
        altitude = parts[11]
        lat = parts[14]
        lon = parts[15]

        if altitude and lat and lon:
            print(f"{flight or icao}: {altitude} ft at {lat}, {lon}")