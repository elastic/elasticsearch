/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.script;

import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.script.Metadata;

import java.time.ZonedDateTime;

/**
 * An external bridge for {@link Metadata}
 */
public interface MetadataBridge extends StableBridgeAPI<Metadata> {

    String getIndex();

    void setIndex(String index);

    String getId();

    void setId(String id);

    long getVersion();

    void setVersion(long version);

    String getVersionType();

    void setVersionType(String versionType);

    String getRouting();

    void setRouting(String routing);

    ZonedDateTime getNow();

    static MetadataBridge fromInternal(final Metadata metadata) {
        return new ProxyInternal(metadata);
    }

    /**
     * An implementation of {@link MetadataBridge} that proxies calls through
     * to an internal {@link Metadata}.
     * @see StableBridgeAPI.ProxyInternal
     */
    final class ProxyInternal extends StableBridgeAPI.ProxyInternal<Metadata> implements MetadataBridge {
        ProxyInternal(final Metadata delegate) {
            super(delegate);
        }

        public String getIndex() {
            return internalDelegate.getIndex();
        }

        public void setIndex(final String index) {
            internalDelegate.setIndex(index);
        }

        public String getId() {
            return internalDelegate.getId();
        }

        public void setId(final String id) {
            internalDelegate.setId(id);
        }

        public long getVersion() {
            return internalDelegate.getVersion();
        }

        public void setVersion(final long version) {
            internalDelegate.setVersion(version);
        }

        public String getVersionType() {
            return internalDelegate.getVersionType();
        }

        public void setVersionType(final String versionType) {
            internalDelegate.setVersionType(versionType);
        }

        public String getRouting() {
            return internalDelegate.getRouting();
        }

        public void setRouting(final String routing) {
            internalDelegate.setRouting(routing);
        }

        public ZonedDateTime getNow() {
            return internalDelegate.getNow();
        }
    }
}
