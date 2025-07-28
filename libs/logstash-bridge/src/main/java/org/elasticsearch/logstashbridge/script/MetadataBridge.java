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
public class MetadataBridge extends StableBridgeAPI.ProxyInternal<Metadata> {
    public MetadataBridge(final Metadata delegate) {
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
