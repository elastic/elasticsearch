/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * A typed reference to a {@link DataSource} by name. Carries only the name — resolution to the full
 * {@link DataSource} object happens against {@link DataSourceMetadata} in cluster state at query or
 * service-layer time. Modelled after {@link org.elasticsearch.index.Index}, which is the precedent
 * for a named reference to a first-class cluster-state entity.
 *
 * <p>The typed wrapper exists so that a field declared as "the name of a data source" (e.g. on
 * {@link Dataset}) is distinguishable at compile time from a plain String, which catches a whole
 * class of mistakes if a new write path ever lands that would otherwise accept any String.
 *
 * <p>No standalone XContent: {@link Dataset#toXContent} emits the reference as a bare string field
 * via {@link #getName()} and {@link Dataset#PARSER} reads it back as a String, wrapping the parsed
 * value here in the constructor lambda. Adding standalone {@code toXContent}/{@code fromXContent}
 * here would create a second JSON format (a nested object) that nothing calls, and would diverge
 * from the actual on-disk shape — a footgun for a future caller that reached for them.
 */
public class DataSourceReference implements Writeable {

    private final String name;

    public DataSourceReference(String name) {
        this.name = Objects.requireNonNull(name, "data source reference name must not be null");
    }

    public DataSourceReference(StreamInput in) throws IOException {
        this.name = in.readString();
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "[" + name + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSourceReference that = (DataSourceReference) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
    }
}
