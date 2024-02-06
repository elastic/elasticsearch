/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Represents a reduced view into an {@link org.elasticsearch.action.datastreams.lifecycle.ErrorEntry}, removing the
 * exception message and last occurrence timestamp as we could potentially send thousands of entries over the wire
 * and the omitted fields would not be used.
 */
public record DslErrorInfo(String indexName, long firstOccurrence, int retryCount) implements Writeable {

    public DslErrorInfo(StreamInput in) throws IOException {
        this(in.readString(), in.readLong(), in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeLong(firstOccurrence);
        out.writeVInt(retryCount);
    }
}
