/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Objects;

public class MonitoringBulkResponse extends ActionResponse {

    private long tookInMillis;
    private Error error;

    MonitoringBulkResponse() {
    }

    public MonitoringBulkResponse(long tookInMillis) {
        this(tookInMillis, null);
    }

    public MonitoringBulkResponse(long tookInMillis, Error error) {
        this.tookInMillis = tookInMillis;
        this.error = error;
    }

    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    public long getTookInMillis() {
        return tookInMillis;
    }

    /**
     * Returns HTTP status
     * <ul>
     * <li>{@link RestStatus#OK} if monitoring bulk request was successful</li>
     * <li>{@link RestStatus#INTERNAL_SERVER_ERROR} if monitoring bulk request was partially successful or failed completely</li>
     * </ul>
     */
    public RestStatus status() {
        return error == null ? RestStatus.OK : RestStatus.INTERNAL_SERVER_ERROR;
    }

    public Error getError() {
        return error;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tookInMillis = in.readVLong();
        error = in.readOptionalWriteable(Error::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(tookInMillis);
        out.writeOptionalWriteable(error);
    }

    public static class Error implements Writeable, ToXContent {

        private final Throwable cause;
        private final RestStatus status;

        public Error(Throwable t) {
            cause = Objects.requireNonNull(t);
            status = ExceptionsHelper.status(t);
        }

        Error(StreamInput in) throws IOException {
            this(in.readException());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeException(getCause());
        }

        /**
         * The failure message.
         */
        public String getMessage() {
            return this.cause.toString();
        }

        /**
         * The rest status.
         */
        public RestStatus getStatus() {
            return this.status;
        }

        /**
         * The actual cause of the failure.
         */
        public Throwable getCause() {
            return cause;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            ElasticsearchException.generateThrowableXContent(builder, params, cause);
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Error [");
            sb.append("cause=").append(cause);
            sb.append(", status=").append(status);
            sb.append(']');
            return sb.toString();
        }
    }
}
