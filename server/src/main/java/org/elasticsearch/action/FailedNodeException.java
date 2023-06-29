/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class FailedNodeException extends ElasticsearchException {

    private final String nodeId;

    public FailedNodeException(String nodeId, String msg, Throwable cause) {
        super(msg, cause);
        this.nodeId = nodeId;
    }

    public String nodeId() {
        return this.nodeId;
    }

    public FailedNodeException(StreamInput in) throws IOException {
        super(in);
        nodeId = in.readOptionalString();
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeOptionalString(nodeId);
    }

    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("node_id", nodeId);
    }

        @Override
        public boolean equals(Object o) {
            return super.equals(o);
//            if (this == o) return true;
//            if (o == null || getClass() != o.getClass()) return false;
//            FailedNodeException that = (FailedNodeException) o;
//            return Objects.equals(nodeId, that.nodeId)
//                && getRootCause().getClass() == that.getRootCause().getClass()
//                && Objects.equals(getMessage(), that.getMessage())
//                && getCause() == that.getCause();
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                nodeId,
                getMessage(),
                getClass(),
                getCause().getMessage(),
                getCause().getClass()
            ); //TODO
        }
}
