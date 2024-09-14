/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class DeletePipelineRequest extends AcknowledgedRequest<DeletePipelineRequest> {

    private String id;

    public DeletePipelineRequest(String id) {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
        if (id == null) {
            throw new IllegalArgumentException("id is missing");
        }
        this.id = id;
    }

    public DeletePipelineRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
    }

    public void setId(String id) {
        this.id = Objects.requireNonNull(id);
    }

    public String getId() {
        return id;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
    }
}
