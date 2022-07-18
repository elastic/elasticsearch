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
    public static final long INTERNAL_VERSION = Long.MIN_VALUE;

    static final Map<String, Metadata.FieldProperty<?>> PROPERTIES = Map.of(
        INDEX,
        RW_STRING,
        ID,
        RW_NULLABLE_STRING,
        VERSION,
        new FieldProperty<>(
            Number.class,
            false,
            false,
            (k, v) -> {
                FieldProperty.LONGABLE_NUMBER.accept(k, v);
                if (v.longValue() < 0 && v.longValue() != Long.MIN_VALUE) {
                    throw new IllegalArgumentException(k + " may only be internal or non-negative, not [" + v + "]");
                }
            }
        ),
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
        Number version = getNumber(VERSION);
        if (version == null) {
            return INTERNAL_VERSION;
        }
        return version.longValue();
    }

    @Override
    public void setVersion(long version) {
        super.setVersion(version);
    }

    public boolean isInternalVersion() {
        return getVersion() == INTERNAL_VERSION;
    }

    public boolean versionChanged() {
        Number updated = getNumber(VERSION);
        if (version == null) {
            return updated == null;
        }
        return version == updated.longValue();
    }

    public boolean indexChanged() {
        return Objects.equals(index, getString(INDEX));
    }

    public boolean idChanged() {
        return Objects.equals(id, getString(ID));
    }

    public boolean routingChanged() {
        return Objects.equals(routing, getString(ROUTING));
    }
}
