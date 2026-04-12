/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.iplocation.api.DatabaseProperty;
import org.elasticsearch.iplocation.api.IpLocationInfoCollector;

import java.io.IOException;
import java.util.Set;

interface InternalIpDataLookup {
    /**
     * Gets data from the provided {@code ipDatabase} for the provided {@code ip} and pushes results to the collector.
     *
     * @param ipDatabase the database from which to lookup a result
     * @param ip the ip address
     * @param collector the collector to push results to
     * @return true if data was found, false if the IP was not found in the database
     * @throws IOException if the implementation encounters any problem while retrieving the response
     */
    boolean getData(IpDatabase ipDatabase, String ip, IpLocationInfoCollector collector) throws IOException;

    /**
     * @return the set of properties this lookup will provide
     */
    Set<DatabaseProperty> getProperties();

    /**
     * A helper record that holds other records. Every ip data lookup will have an associated ip address that was looked up, as well
     * as a network for which the  record applies. Having a helper record prevents each individual response record from needing to
     * track these bits of information.
     */
    record Result<T>(T result, String ip, String network) {}
}
