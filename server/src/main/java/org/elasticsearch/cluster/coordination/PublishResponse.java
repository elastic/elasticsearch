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
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Response to a {@link PublishRequest}, carrying the term and version of the request.
 * Typically wrapped in a {@link PublishWithJoinResponse}.
 */
public class PublishResponse implements Writeable {

    private final long term;
    private final long version;

    public PublishResponse(long term, long version) {
        assert term >= 0;
        assert version >= 0;

        this.term = term;
        this.version = version;
    }

    public PublishResponse(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(term);
        out.writeLong(version);
    }

    public long getTerm() {
        return term;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "PublishResponse{" +
            "term=" + term +
            ", version=" + version +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PublishResponse response = (PublishResponse) o;

        if (term != response.term) return false;
        return version == response.version;
    }

    @Override
    public int hashCode() {
        int result = (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (version ^ (version >>> 32));
        return result;
    }
}
