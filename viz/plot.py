from influxdb_client import InfluxDBClient
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import sys
from datetime import datetime

def visualize_temperature_animated(influx_url: str, token: str, org: str, bucket: str,
                                   range_start: str = "-1h", limit_points: int = 50,
                                   update_interval: int = 5000):
   
    
   
    fig, ax = plt.subplots(figsize=(10, 5))
    line, = ax.plot([], [], marker='o', linestyle='-', color='b')
    
    
    ax.set_title("Évolution de la température des capteurs - Temps réel")
    ax.set_xlabel("Temps")
    ax.set_ylabel("Température (°C)")
    ax.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Variables pour stocker les données et la connexion
    client = None
    query_api = None
    
    def init_connection():
        """Initialise la connexion à InfluxDB"""
        nonlocal client, query_api
        try:
            client = InfluxDBClient(url=influx_url, token=token, org=org)
            query_api = client.query_api()
            print("Connexion InfluxDB réussie")
            return True
        except Exception as e:
            print(f"Erreur connexion InfluxDB : {e}")
            return False
    
    def fetch_data():
       
        if not query_api:
            return pd.DataFrame()
        
        query = f'''
        from(bucket: "{bucket}")
          |> range(start: {range_start})
          |> filter(fn: (r) => r["_measurement"] == "capteur")
          |> filter(fn: (r) => r["_field"] == "temperature")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: {limit_points})
        '''
        
        try:
            result = query_api.query(org=org, query=query)
            
            # Conversion en DataFrame
            records = [{"timestamp": rec.get_time(), "temperature": rec.get_value()}
                      for table in result for rec in table.records]
            
            if not records:
                print(f"{datetime.now().strftime('%H:%M:%S')} - Aucune donnée trouvée")
                return pd.DataFrame()
            
            df = pd.DataFrame(records).sort_values("timestamp")
            
            # Conversion du temps  
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert('Africa/Algiers')
  
            
            return df
            
        except Exception as e:
            print(f"Erreur lors de la récupération des données : {e}")
            return pd.DataFrame()
    
    def init_animation():
        """Fonction d'initialisation pour l'animation"""
        return line,
    
    def update(frame):
        """Fonction de mise à jour pour l'animation"""
        # Récupère les nouvelles données
        df = fetch_data()
        
        if df.empty:
            # Garde l'ancien graphique si pas de nouvelles données
            return line,
        
        # Met à jour les données du graphique
        line.set_data(df["timestamp"], df["temperature"])
        
        # Ajuste les limites de l'axe X pour suivre le temps
        if len(df) > 0:
            ax.set_xlim(df["timestamp"].min(), df["timestamp"].max())
        
        # Ajuste les limites de l'axe Y avec une marge de 5%
        if len(df) > 0:
            y_min, y_max = df["temperature"].min(), df["temperature"].max()
            y_margin = (y_max - y_min) * 0.05 if y_max != y_min else 1
            ax.set_ylim(y_min - y_margin, y_max + y_margin)
        
        # Met à jour le titre avec l'heure de dernière mise à jour
        last_update = datetime.now().strftime("%H:%M:%S")
        ax.set_title(f"Évolution de la température des capteurs - Dernière mise à jour: {last_update}")
        
        # Force la mise à jour des ticks
        ax.figure.canvas.draw_idle()
        
        return line,
    
    def on_close(event):
       
        if client:
            client.close()
            print("Connexion InfluxDB fermée")
    
    # Initialise la connexion
    if not init_connection():
        sys.exit(1)
    
    # Configure l'événement de fermeture
    fig.canvas.mpl_connect('close_event', on_close)
    
  
    ani = animation.FuncAnimation(
        fig=fig,
        func=update,
        init_func=init_animation,  
        interval=update_interval,  
        blit=False,  
        cache_frame_data=False
    )
    
    plt.show()
    
   
    if client:
        client.close()

def visualize_temperature_static(influx_url: str, token: str, org: str, bucket: str,
                                 range_start: str = "-1h", limit_points: int = 50):
   
    try:
        client = InfluxDBClient(url=influx_url, token=token, org=org)
        query_api = client.query_api()
        print("Connexion InfluxDB réussie")
    except Exception as e:
        print("Erreur connexion InfluxDB :", e)
        sys.exit(1)

    query = f'''
from(bucket: "{bucket}")
  |> range(start: {range_start})
  |> filter(fn: (r) => r["_measurement"] == "capteur")
  |> filter(fn: (r) => r["_field"] == "temperature")
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: {limit_points})
'''

    try:
        result = query_api.query(org=org, query=query)
    except Exception as e:
        print("Erreur lors de l'exécution de la query :", e)
        client.close()
        sys.exit(1)

    records = [{"timestamp": rec.get_time(), "temperature": rec.get_value()}
               for table in result for rec in table.records]

    if not records:
        print("Aucune donnée trouvée dans InfluxDB")
        client.close()
        return

    df = pd.DataFrame(records).sort_values("timestamp")

    # Conversion du temps
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert('Africa/Algiers')

   
    plt.figure(figsize=(10, 5))
    plt.plot(df["timestamp"], df["temperature"], marker='o')
    plt.title("Évolution de la température des capteurs")
    plt.xlabel("Temps")
    plt.ylabel("Température (°C)")
    
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    client.close()
    print("Connexion InfluxDB fermée")

if __name__ == "__main__":
    
    config = {
        "influx_url": "http://localhost:8086",
        "token": "b2cWipS1p4G2a_dXPrx2qjndLcD6gqHG73oE0auDUyoxwfpPYtcbQ6NDMtMzYezXvfOvFQYiyolHcjwBdexU0A==",
        "org": "myorg",
        "bucket": "capteurs",
        "range_start": "-1h",
        "limit_points": 50
    }
    
    # Choix du mode
    mode = input("Choisissez le mode (1: Statique, 2: Animé) : ").strip()
    
    if mode == "1":
        print("Lancement de la visualisation statique...")
        visualize_temperature_static(**config)
    else:
        print("Lancement de la visualisation animée...")
        try:
            interval = int(input("Intervalle de mise à jour en ms : ").strip() or "5000")
            visualize_temperature_animated(**config, update_interval=interval)
        except ValueError:
            print("Valeur invalide, utilisation de l'intervalle par défaut (5000ms)")
            visualize_temperature_animated(**config, update_interval=5000)