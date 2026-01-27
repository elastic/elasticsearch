/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.geoip;

import com.maxmind.db.Reader;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.ingest.geoip.IpDatabase;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.core.CheckedBiFunctionBridge;

import java.io.IOException;

/**
 * An {@link StableBridgeAPI} for {@link IpDatabase}
 */
public interface IpDatabaseBridge extends StableBridgeAPI<IpDatabase> {

    String getDatabaseType() throws IOException;

    @Nullable
    <RESPONSE> RESPONSE getResponse(String ipAddress, CheckedBiFunctionBridge<Reader, String, RESPONSE, Exception> responseProvider);

    void close() throws IOException;

}
