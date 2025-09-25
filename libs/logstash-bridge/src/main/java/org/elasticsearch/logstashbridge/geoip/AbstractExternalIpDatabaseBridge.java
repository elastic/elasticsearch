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

import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.ingest.geoip.IpDatabase;

import java.io.IOException;

/**
 * The {@code IpDatabaseBridge.AbstractExternal} is an abstract base class for implementing
 * the {@link IpDatabaseBridge} externally to the Elasticsearch code-base. It takes care of
 * the details of maintaining a singular internal-form implementation of {@link IpDatabase}
 * that proxies calls to the external implementation.
 */
public abstract class AbstractExternalIpDatabaseBridge implements IpDatabaseBridge {
    private ProxyExternal internalDatabase;

    @Override
    public IpDatabase toInternal() {
        if (internalDatabase == null) {
            internalDatabase = new ProxyExternal();
        }
        return internalDatabase;
    }

    /**
     * An implementation of {@link IpDatabase} that proxies calls to
     * a bridged {@link AbstractExternalIpDatabaseBridge} instance.
     */
    private final class ProxyExternal implements IpDatabase {

        @Override
        public String getDatabaseType() throws IOException {
            return AbstractExternalIpDatabaseBridge.this.getDatabaseType();
        }

        @Override
        public <RESPONSE> RESPONSE getResponse(String ipAddress, CheckedBiFunction<Reader, String, RESPONSE, Exception> responseProvider) {
            return AbstractExternalIpDatabaseBridge.this.getResponse(ipAddress, responseProvider::apply);
        }

        @Override
        public void close() throws IOException {
            AbstractExternalIpDatabaseBridge.this.close();
        }
    }
}
