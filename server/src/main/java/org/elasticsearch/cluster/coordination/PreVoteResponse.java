/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

public class PreVoteResponse extends TransportResponse {
    private final long currentTerm;
    private final long lastAcceptedTerm;
    private final long lastAcceptedVersion;

    public PreVoteResponse(long currentTerm, long lastAcceptedTerm, long lastAcceptedVersion) {
        this.currentTerm = currentTerm;
        this.lastAcceptedTerm = lastAcceptedTerm;
        this.lastAcceptedVersion = lastAcceptedVersion;
        assert lastAcceptedTerm <= currentTerm : currentTerm + " < " + lastAcceptedTerm;
    }

    public PreVoteResponse(StreamInput in) throws IOException {
        currentTerm = in.readLong();
        lastAcceptedTerm = in.readLong();
        lastAcceptedVersion = in.readLong();
        assert lastAcceptedTerm <= currentTerm : currentTerm + " < " + lastAcceptedTerm;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(currentTerm);
        out.writeLong(lastAcceptedTerm);
        out.writeLong(lastAcceptedVersion);
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public long getLastAcceptedTerm() {
        return lastAcceptedTerm;
    }

    public long getLastAcceptedVersion() {
        return lastAcceptedVersion;
    }

    @Override
    public String toString() {
        return "PreVoteResponse{" +
            "currentTerm=" + currentTerm +
            ", lastAcceptedTerm=" + lastAcceptedTerm +
            ", lastAcceptedVersion=" + lastAcceptedVersion +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PreVoteResponse that = (PreVoteResponse) o;
        return currentTerm == that.currentTerm &&
            lastAcceptedTerm == that.lastAcceptedTerm &&
            lastAcceptedVersion == that.lastAcceptedVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentTerm, lastAcceptedTerm, lastAcceptedVersion);
    }
}
