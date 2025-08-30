/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.geoip;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logstashbridge.StableBridgeAPI;

import java.io.File;
import java.io.IOException;

public interface MaxMindDbBridge {

    class Reader extends StableBridgeAPI.ProxyInternal<com.maxmind.db.Reader> {

        @SuppressForbidden(reason = "Maxmind Reader constructor requires java.io.File")
        public Reader(final File databasePath, final NodeCache nodeCache) throws IOException {
            super(new com.maxmind.db.Reader(databasePath, nodeCache.toInternal()));
        }

        protected Reader(final com.maxmind.db.Reader internalDelegate) {
            super(internalDelegate);
        }

        @Override
        public com.maxmind.db.Reader toInternal() {
            return internalDelegate;
        }

        public String getDatabaseType() {
            return toInternal().getMetadata().getDatabaseType();
        }

        public void close() throws IOException {
            toInternal().close();
        }
    }

    class NodeCache extends StableBridgeAPI.ProxyInternal<com.maxmind.db.NodeCache> {

        protected NodeCache(final com.maxmind.db.NodeCache internalDelegate) {
            super(internalDelegate);
        }

        public static NodeCache get(final int capacity) {
            return new NodeCache(new com.maxmind.db.CHMCache(capacity));
        }

        public static NodeCache getInstance() {
            return new NodeCache(com.maxmind.db.NoCache.getInstance());
        }
    }

}
