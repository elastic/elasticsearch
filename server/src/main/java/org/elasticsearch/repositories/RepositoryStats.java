/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class RepositoryStats implements Writeable {

    public static final RepositoryStats EMPTY_STATS = new RepositoryStats(Collections.emptyMap());

    public final Map<String, Long> requestCounts;

    public RepositoryStats(Map<String, Long> requestCounts) {
        this.requestCounts = Collections.unmodifiableMap(requestCounts);
    }

    public RepositoryStats(StreamInput in) throws IOException {
        this.requestCounts = in.readMap(StreamInput::readLong);
    }

    public RepositoryStats merge(RepositoryStats otherStats) {
        final Map<String, Long> result = new HashMap<>();
        result.putAll(requestCounts);
        for (Map.Entry<String, Long> entry : otherStats.requestCounts.entrySet()) {
            result.merge(entry.getKey(), entry.getValue(), Math::addExact);
        }
        return new RepositoryStats(result);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(requestCounts, StreamOutput::writeString, StreamOutput::writeLong);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepositoryStats that = (RepositoryStats) o;
        return requestCounts.equals(that.requestCounts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestCounts);
    }

    @Override
    public String toString() {
        return "RepositoryStats{" + "requestCounts=" + requestCounts + '}';
    }
}
