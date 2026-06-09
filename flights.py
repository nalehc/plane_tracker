from geopy.distance import geodesic

class Aircraft():
    def __init__(self, icao):
        self.icao = icao
        self.data = {"icao": icao}

    def update(self, record):
        for key, value in record.items():
            if value is not None:
                self.data[key] = value
        self.data["last_seen_utc"] = record["received_at_utc"]
        self.data["last_seen_local"] = record["received_at_local"]

    def update_distance(self, home_lat, home_lon):
        lat = self.data.get("lat")
        lon = self.data.get("lon")

        if lat is None or lon is None:
            self.data["distance_mi"] = None
            return

        self.data["distance_mi"] = geodesic(
            (home_lat, home_lon),
            (lat, lon),
        ).miles
    
    def is_visible(self, max_distance, max_alt):
        return (
            self.data.get("distance_mi") is not None
            and self.data.get("altitude_ft") is not None
            and self.data["distance_mi"] < max_distance
            and self.data["altitude_ft"] < max_alt
        )


class CurrentFlights():
    def __init__(self):
        self.data = {}
        
    def update(self, record):
        icao = record["icao"]
        if not icao:
            return

        aircraft = self.data.setdefault(icao, Aircraft(icao))
        aircraft.update(record)

    def update_distances(self, home_lat, home_lon):
        for aircraft in self.data.values():
            aircraft.update_distance(home_lat, home_lon)

    def records(self):
        return [aircraft.data for aircraft in self.data.values()]

    def visible(self, max_distance, max_alt):
        return [
            aircraft
            for aircraft in self.data.values()
            if aircraft.is_visible(max_distance, max_alt)
        ]
