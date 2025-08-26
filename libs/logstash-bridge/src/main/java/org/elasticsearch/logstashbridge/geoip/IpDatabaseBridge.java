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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.ingest.geoip.IpDatabase;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.core.CheckedBiFunctionBridge;

import java.io.IOException;

/**
 * An external bridge for {@link IpDatabase}
 */
public interface IpDatabaseBridge extends StableBridgeAPI<IpDatabase> {

    String getDatabaseType() throws IOException;

    @Nullable
    <RESPONSE> RESPONSE getResponse(String ipAddress, CheckedBiFunctionBridge<Reader, String, RESPONSE, Exception> responseProvider);

    void close() throws IOException;

    /**
     * The {@code IpDatabaseBridge.AbstractExternal} is an abstract base class for implementing
     * the {@link IpDatabaseBridge} externally to the Elasticsearch code-base. It takes care of
     * the details of maintaining a singular internal-form implementation of {@link IpDatabase}
     * that proxies calls through the external implementation.
     */
    abstract class AbstractExternal implements IpDatabaseBridge {
        private ProxyExternal internalDatabase;

        @Override
        public IpDatabase toInternal() {
            if (internalDatabase == null) {
                internalDatabase = new ProxyExternal();
            }
            return internalDatabase;
        }

        private class ProxyExternal implements IpDatabase {

            @Override
            public String getDatabaseType() throws IOException {
                return AbstractExternal.this.getDatabaseType();
            }

            @Override
            public <RESPONSE> RESPONSE getResponse(
                String ipAddress,
                CheckedBiFunction<Reader, String, RESPONSE, Exception> responseProvider
            ) {
                return AbstractExternal.this.getResponse(ipAddress, responseProvider::apply);
            }

            @Override
            public void close() throws IOException {
                AbstractExternal.this.close();
            }
        }
    }
}
