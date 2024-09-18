/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

interface GeoDataLookup {
    /**
     * Gets geodata from the provided {@code geoIpDatabase} for the provided {@code ip}
     * @param geoIpDatabase the database from which to lookup a result
     * @param ip the ip address
     * @return a map of geodata corresponding to the configured properties
     * @throws IOException if the implementation encounters any problem while retrieving the response
     */
    Map<String, Object> getGeoData(IpDatabase geoIpDatabase, String ip) throws IOException;

    /**
     * @return the set of properties this lookup will provide
     */
    Set<Database.Property> getProperties();
}
