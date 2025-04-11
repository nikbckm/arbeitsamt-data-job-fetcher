# Arbeitssamt Data Job Fetcher

## Beschreibung
Dieses Projekt automatisiert das Abrufen von Jobangeboten von der Jobbörse der Bundesagentur für Arbeit (Arbeitsagentur) mit dem Keyword "Data" und speichert die Daten in einer CSV-Datei. Der Fetcher läuft einmal täglich und fügt neue Jobs hinzu, die noch nicht in der Datei vorhanden sind. Es wird auch das Datum der Datensammlung gespeichert, um eine Nachverfolgbarkeit zu gewährleisten.

## Funktionsweise
- **API Anbindung**: Es wird eine Verbindung zur Jobbörse API hergestellt, um Jobangebote zu laden.
- **CSV-Datenbank**: Alle abgerufenen Jobangebote werden in einer CSV-Datei (`job_details.csv`) gespeichert.
- **Tägliche Aktualisierung**: Jeden Tag wird ein neuer Fetch gestartet, der neue Jobs hinzufügt, sofern diese nicht bereits vorhanden sind.
- **GitHub Actions**: Automatisierte Ausführung über GitHub Actions, die einmal pro Tag ausgeführt wird.

## GitHub Actions
- GitHub Actions führt das Skript täglich um 00:00 UTC automatisch aus.
- Jede neue Job-ID wird der `job_details.csv` hinzugefügt und mit dem aktuellen Datum versehen (`scraping_date`).
- Alle Änderungen an der CSV-Datei werden automatisch an das GitHub-Repository zurückgepusht.

## bundesAPI / Jobsuche API
- https://github.com/bundesAPI/jobsuche-api
- https://jobsuche.api.bund.dev/

## Lizenz
Dieses Projekt ist unter der MIT-Lizenz lizenziert.
