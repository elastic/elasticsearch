# Tableau connector for Elasticsearch

The Tableau Connector works in tandem with the Elastic JDBC driver to facilitate the query of Elasticsearch. It gives users a simple way to query Elasticsearch data from Tableau.
After providing basic connection and authentication information, users can easily select Elasticsearch indices for use in Tableau Desktop and Tableau Server.

## Installation

1. Tableau connector for Elasticsearch installation:
 - Go to the [Connector Download](https://www.elastic.co/downloads/tableau-connector) page.
 - Download the _.taco_ connector file.
 - Move the _.taco_ file here:
    - Windows: C:\Users\[Windows User]\Documents\My Tableau Repository\Connectors
    -  macOS: /Users/[user]/Documents/My Tableau Repository/Connectors
2. Elasticsearch JDBC Driver Installation:
 - Go to the [Driver Download](https://www.elastic.co/downloads/jdbc-client) page.
 - Download the Elasticsearch JDBC Driver _.jar_ file and move it into the following directory:
    - Windows - C:\Program Files\Tableau\Drivers
    - macOS - /Users/[user]/Library/Tableau/Drivers
3. Relaunch Tableau and connect with _Elasticsearch by Elastic_.
