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

public class MetadataBridge extends StableBridgeAPI.Proxy<Metadata> {
    public MetadataBridge(final Metadata delegate) {
        super(delegate);
    }

    public String getIndex() {
        return delegate.getIndex();
    }

    public void setIndex(final String index) {
        delegate.setIndex(index);
    }

    public String getId() {
        return delegate.getId();
    }

    public void setId(final String id) {
        delegate.setId(id);
    }

    public long getVersion() {
        return delegate.getVersion();
    }

    public void setVersion(final long version) {
        delegate.setVersion(version);
    }

    public String getVersionType() {
        return delegate.getVersionType();
    }

    public void setVersionType(final String versionType) {
        delegate.setVersionType(versionType);
    }

    public String getRouting() {
        return delegate.getRouting();
    }

    public void setRouting(final String routing) {
        delegate.setRouting(routing);
    }

    public ZonedDateTime getNow() {
        return delegate.getNow();
    }
}
