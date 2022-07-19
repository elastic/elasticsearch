/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Objects;
import java.util.Set;

public class ReindexMetadata extends Metadata {
    private static final Metadata.FieldProperty<String> WritableStringField = StringField.withWritable();
    public static final long INTERNAL_VERSION = Long.MIN_VALUE;

    protected final String index;
    protected final String id;
    protected final Long version;
    protected final String routing;

    public ReindexMetadata(String index, String id, Long version, String routing, String op, long timestamp) {
        super(
            new MetadataBuilder(6).index(index, WritableStringField)
                .id(id, WritableStringField.withNullable())
                .version(version, LongField.withWritable().withNullable().withValidation((k, v) -> {
                    LongField.extendedValidation().accept(k, v);
                    if (v.longValue() < 0 && v.longValue() != Long.MIN_VALUE) {
                        throw new IllegalArgumentException(k + " may only be internal or non-negative, not [" + v + "]");
                    }
                }))
                .routing(routing, WritableStringField.withNullable())
                .op(op, WritableStringSetField(Set.of("noop", "index", "delete")))
                .timestamp(timestamp, LongField)
        );
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
        if (version == INTERNAL_VERSION) {
            put(VERSION, null);
        } else {
            super.setVersion(version);
        }
    }

    public boolean isInternalVersion() {
        return getVersion() == INTERNAL_VERSION;
    }

    public boolean versionChanged() {
        Number updated = getNumber(VERSION);
        if (version == null || updated == null) {
            return version != updated;
        }
        return version != updated.longValue();
    }

    public boolean indexChanged() {
        return Objects.equals(index, getString(INDEX)) == false;
    }

    public boolean idChanged() {
        return Objects.equals(id, getString(ID)) == false;
    }

    public boolean routingChanged() {
        return Objects.equals(routing, getString(ROUTING)) == false;
    }
}
