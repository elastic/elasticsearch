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
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

abstract class TermVersionRequest extends TransportRequest implements Writeable {
    protected final DiscoveryNode sourceNode;
    protected final long term;
    protected final long version;

    TermVersionRequest(DiscoveryNode sourceNode, long term, long version) {
        assert term >= 0;
        assert version >= 0;

        this.sourceNode = sourceNode;
        this.term = term;
        this.version = version;
    }

    TermVersionRequest(StreamInput in) throws IOException {
        super(in);
        sourceNode = new DiscoveryNode(in);
        term = in.readLong();
        version = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceNode.writeTo(out);
        out.writeLong(term);
        out.writeLong(version);
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public long getTerm() {
        return term;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TermVersionRequest versionTerm = (TermVersionRequest) o;

        if (term != versionTerm.term) return false;
        if (version != versionTerm.version) return false;
        return sourceNode.equals(versionTerm.sourceNode);
    }

    @Override
    public int hashCode() {
        int result = (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (version ^ (version >>> 32));
        result = 31 * result + sourceNode.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TermVersionRequest{" +
            "term=" + term +
            ", version=" + version +
            ", sourceNode=" + sourceNode +
            '}';
    }
}
