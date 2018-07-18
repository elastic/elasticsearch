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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public class Join implements Writeable {
    private final DiscoveryNode sourceNode;
    private final DiscoveryNode targetNode;
    private final long term;
    private final long lastAcceptedTerm;
    private final long lastAcceptedVersion;

    public Join(DiscoveryNode sourceNode, DiscoveryNode targetNode, long term, long lastAcceptedTerm, long lastAcceptedVersion) {
        assert term >= 0;
        assert lastAcceptedTerm >= 0;
        assert lastAcceptedVersion >= 0;

        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.term = term;
        this.lastAcceptedTerm = lastAcceptedTerm;
        this.lastAcceptedVersion = lastAcceptedVersion;
    }

    public Join(StreamInput in) throws IOException {
        sourceNode = new DiscoveryNode(in);
        targetNode = new DiscoveryNode(in);
        term = in.readLong();
        lastAcceptedTerm = in.readLong();
        lastAcceptedVersion = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        sourceNode.writeTo(out);
        targetNode.writeTo(out);
        out.writeLong(term);
        out.writeLong(lastAcceptedTerm);
        out.writeLong(lastAcceptedVersion);
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    public long getLastAcceptedVersion() {
        return lastAcceptedVersion;
    }

    public long getTerm() {
        return term;
    }

    public long getLastAcceptedTerm() {
        return lastAcceptedTerm;
    }

    @Override
    public String toString() {
        return "Join{" +
            "term=" + term +
            ", lastAcceptedTerm=" + lastAcceptedTerm +
            ", lastAcceptedVersion=" + lastAcceptedVersion +
            ", sourceNode=" + sourceNode +
            ", targetNode=" + targetNode +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Join join = (Join) o;

        if (sourceNode.equals(join.sourceNode) == false) return false;
        if (targetNode.equals(join.targetNode) == false) return false;
        if (lastAcceptedVersion != join.lastAcceptedVersion) return false;
        if (term != join.term) return false;
        return lastAcceptedTerm == join.lastAcceptedTerm;
    }

    @Override
    public int hashCode() {
        int result = (int) (lastAcceptedVersion ^ (lastAcceptedVersion >>> 32));
        result = 31 * result + sourceNode.hashCode();
        result = 31 * result + targetNode.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (lastAcceptedTerm ^ (lastAcceptedTerm >>> 32));
        return result;
    }
}
