/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Map;
import java.util.Objects;

public class ReindexMetadata extends BulkMetadata {
    static final Map<String, Metadata.FieldProperty<?>> PROPERTIES = Map.of(
        INDEX,
        RW_STRING,
        ID,
        RW_NULLABLE_STRING,
        VERSION,
        RW_NULLABLE_LONG,
        ROUTING,
        RW_NULLABLE_STRING,
        OP,
        OP_PROPERTY,
        TIMESTAMP,
        RO_LONG
    );

    protected final String index;
    protected final String id;
    protected final Long version;
    protected final String routing;

    public ReindexMetadata(String index, String id, Long version, String routing, String op, long timestamp) {
        super(index, id, version, routing, op, timestamp, PROPERTIES);
        this.index = index;
        this.id = id;
        this.version = version;
        this.routing = routing;
    }

    @Override
    public long getVersion() {
        if (get(VERSION) == null) {
            return Long.MIN_VALUE;
        }
        return super.getVersion();
    }

    @Override
    public void setVersion(long version) {
        super.setVersion(version);
    }

    public boolean isInternalVersion() {
        return get(VERSION) != null;
    }

    public void setInternalVersion() {
        put(VERSION, null);
    }

    public boolean indexChanged() {
        return Objects.equals(index, getString(INDEX));
    }

    public boolean idChanged() {
        return Objects.equals(id, getString(ID));
    }

    public boolean versionChanged() {
        Number updated = getNumber(VERSION);
        if (version == null) {
            return updated == null;
        }
        return version == updated.longValue();
    }

    public boolean routingChanged() {
        return Objects.equals(routing, getString(ROUTING));
    }
}
