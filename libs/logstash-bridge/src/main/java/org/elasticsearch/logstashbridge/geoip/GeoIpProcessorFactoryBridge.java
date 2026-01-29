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
import org.elasticsearch.logstashbridge.ingest.ProcessorFactoryBridge;

/**
 * A {@link ProcessorFactoryBridge} implementation for the {@link GeoIpProcessor}
 */
public final class GeoIpProcessorFactoryBridge extends ProcessorFactoryBridge.ProxyInternal {

    public static GeoIpProcessorFactoryBridge create(final IpDatabaseProviderBridge bridgedIpDatabaseProvider) {
        return new GeoIpProcessorFactoryBridge(bridgedIpDatabaseProvider);
    }

    GeoIpProcessorFactoryBridge(final IpDatabaseProviderBridge ipDatabaseProviderBridge) {
        super(new GeoIpProcessor.Factory("geoip", ipDatabaseProviderBridge.toInternal()));
    }
}
