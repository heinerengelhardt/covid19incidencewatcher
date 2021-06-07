# covid19incidencewatcher
 Simpler Covid-19 Inzidenz Watcher.

## Genutzte AWS Services
* EventBridge: Zeitlicher Trigger sowie Input der Events als JSON.
* Lambda: Fragt die ArcGIS API ab und persistiert den Datensatz.
* DynamoDB: Hält für einen Landkreis ein JSON File.
* SNS: Notifiziert per Email bei Veränderung der Inzidenz.
