/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

public final class ShardSearchContextId implements Writeable {
    private final String sessionId;
    private final long id;
    private final String searcherId;

    // TODO: Remove this constructor
    public ShardSearchContextId(String sessionId, long id) {
        this(sessionId, id, null);
    }

    public ShardSearchContextId(String sessionId, long id, String searcherId) {
        this.sessionId = Objects.requireNonNull(sessionId);
        this.id = id;
        this.searcherId = searcherId;
    }

    public ShardSearchContextId(StreamInput in) throws IOException {
        this.id = in.readLong();
        this.sessionId = in.readString();
        this.searcherId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(id);
        out.writeString(sessionId);
        out.writeOptionalString(searcherId);

    }

    public String getSessionId() {
        return sessionId;
    }

    public long getId() {
        return id;
    }

    public String getSearcherId() {
        return searcherId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardSearchContextId other = (ShardSearchContextId) o;
        return id == other.id && sessionId.equals(other.sessionId) && Objects.equals(searcherId, other.searcherId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, id, searcherId);
    }

    @Override
    public String toString() {
        return "[" + sessionId + "][" + id + "] searcherId [" + searcherId + "]";
    }
}
