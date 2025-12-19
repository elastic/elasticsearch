/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unique identifier for a named expression.
 * <p>
 * We use an {@link AtomicLong} to guarantee that they are unique
 * and that create reproducible values when run in subsequent
 * tests. They don't produce reproducible values in production, but
 * you rarely debug with them in production and commonly do so in
 * tests.</p>
 */
public class NameId implements Writeable {
    private static final AtomicLong COUNTER = new AtomicLong();
    private final long id;

    public NameId() {
        this.id = COUNTER.incrementAndGet();
    }

    /**
     * Absolutely only intended for tests, esp. to deal with serialization. Never use in production as it breaks the
     * uniqueness guarantee.
     */
    @Deprecated
    public NameId(long id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        NameId other = (NameId) obj;
        return id == other.id;
    }

    @Override
    public String toString() {
        return Long.toString(id);
    }

    public static NameId readFrom(PlanStreamInput in) throws IOException {
        long unmappedId = in.readLong();
        return in.mapNameId(unmappedId);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(id);
    }
}
