/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.geoip;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.geoip.IpDatabase;
import org.elasticsearch.ingest.geoip.IpDatabaseProvider;
import org.elasticsearch.logstashbridge.StableBridgeAPI;

import static org.elasticsearch.logstashbridge.StableBridgeAPI.toInternalNullable;

/**
 * An external bridge for {@link Processor}
 */
public interface IpDatabaseProviderBridge extends StableBridgeAPI<IpDatabaseProvider> {

    Boolean isValid(String name);

    IpDatabaseBridge getDatabase(String name);

    /**
     * The {@code IpDatabaseProviderBridge.AbstractExternal} is an abstract base class for implementing
     * the {@link IpDatabaseProviderBridge} externally to the Elasticsearch code-base. It takes care of
     * the details of maintaining a singular internal-form implementation of {@link IpDatabaseProvider}
     * that proxies calls to the external implementation.
     */
    abstract class AbstractExternal implements IpDatabaseProviderBridge {
        private AbstractExternal.ProxyExternal internalProcessor;

        public IpDatabaseProvider toInternal() {
            if (internalProcessor == null) {
                internalProcessor = new AbstractExternal.ProxyExternal();
            }
            return internalProcessor;
        }

        private class ProxyExternal implements IpDatabaseProvider {

            private AbstractExternal getIpDatabaseProviderBridge() {
                return AbstractExternal.this;
            }

            @Override
            public Boolean isValid(ProjectId projectId, String name) {
                return AbstractExternal.this.isValid(name);
            }

            @Override
            public IpDatabase getDatabase(ProjectId projectId, String name) {
                return toInternalNullable(AbstractExternal.this.getDatabase(name));
            }
        }
    }
}
