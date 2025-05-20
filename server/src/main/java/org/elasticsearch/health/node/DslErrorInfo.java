/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Represents a reduced view into an {@link org.elasticsearch.action.datastreams.lifecycle.ErrorEntry}, removing the
 * exception message and last occurrence timestamp as we could potentially send thousands of entries over the wire
 * and the omitted fields would not be used.
 */
public record DslErrorInfo(String indexName, long firstOccurrence, int retryCount, ProjectId projectId) implements Writeable {

    public DslErrorInfo(String indexName, long firstOccurrence, int retryCount) {
        this(indexName, firstOccurrence, retryCount, ProjectId.DEFAULT);
    }

    public DslErrorInfo(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readLong(),
            in.readVInt(),
            in.getTransportVersion().onOrAfter(TransportVersions.ADD_PROJECT_ID_TO_DSL_ERROR_INFO)
                ? ProjectId.readFrom(in)
                : ProjectId.DEFAULT
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeLong(firstOccurrence);
        out.writeVInt(retryCount);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ADD_PROJECT_ID_TO_DSL_ERROR_INFO)) {
            projectId.writeTo(out);
        }
    }
}
