/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.geoip;

import org.elasticsearch.ingest.geoip.IpDatabaseProvider;
import org.elasticsearch.logstashbridge.StableBridgeAPI;

/**
 * An {@link StableBridgeAPI} for {@link IpDatabaseProvider}
 */
public interface IpDatabaseProviderBridge extends StableBridgeAPI<IpDatabaseProvider> {

    Boolean isValid(String name);

    IpDatabaseBridge getDatabase(String name);

}
