/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
