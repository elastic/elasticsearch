/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.geoip;

import org.elasticsearch.ingest.geoip.GeoIpProcessor;
import org.elasticsearch.ingest.geoip.IpDatabaseProvider;
import org.elasticsearch.logstashbridge.StableBridgeAPI;

/**
 * An external bridge for {@link GeoIpProcessor}
 */
public interface GeoIpProcessorBridge {

    class Factory extends StableBridgeAPI.ProxyInternal<GeoIpProcessor.Factory> {

        public Factory(String type, IpDatabaseProvider ipDatabaseProvider) {
            super(new GeoIpProcessor.Factory(type, ipDatabaseProvider));
        }
    }
}
