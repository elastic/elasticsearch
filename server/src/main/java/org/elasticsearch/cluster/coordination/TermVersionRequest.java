/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;

abstract class TermVersionRequest extends AbstractTransportRequest implements Writeable {
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
        return "TermVersionRequest{" + "term=" + term + ", version=" + version + ", sourceNode=" + sourceNode + '}';
    }
}
