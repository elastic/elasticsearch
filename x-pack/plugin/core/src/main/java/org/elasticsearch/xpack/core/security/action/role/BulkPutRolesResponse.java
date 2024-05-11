/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class BulkPutRolesResponse extends ActionResponse implements ToXContentObject {

    private final List<Item> items;
    private final boolean error;
    private final long tookInMillis;

    public static class Builder {

        private final List<Item> items = new LinkedList<>();
        private boolean error = false;
        private long tookInMillis;

        public Builder addItem(Item item) {
            items.add(item);
            return this;
        }

        public Builder setTook(long tookInMillis) {
            this.tookInMillis = tookInMillis;
            return this;
        }

        public Builder setError(boolean error) {
            this.error = error;
            return this;
        }

        public BulkPutRolesResponse build() {
            return new BulkPutRolesResponse(items, error, tookInMillis);
        }
    }

    public BulkPutRolesResponse(List<Item> items, boolean error, long tookInMillis) {
        this.items = items;
        this.error = error;
        this.tookInMillis = tookInMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("took", tookInMillis);
        builder.field("errors", error);
        builder.startArray("items");
        for (Item item : items) {
            item.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    public static class Item implements Writeable, ToXContentObject {

        private static final String _ID = "_id";
        static final String STATUS = "status";
        static final String ERROR = "error";
        static final String RESULT = "result";
        private final Exception cause;
        private final RestStatus status;
        private final String roleName;
        private final DocWriteResponse.Result resultType;

        private Item(String roleName, DocWriteResponse.Result resultType, RestStatus status, Exception cause) {
            this.roleName = roleName;
            this.resultType = resultType;
            this.status = status;
            this.cause = cause;
        }

        Item(StreamInput in) throws IOException {
            roleName = in.readString();
            resultType = DocWriteResponse.Result.readFrom(in);
            status = RestStatus.readFrom(in);
            cause = in.readException();
        }

        public RestStatus status() {
            return status;
        }

        public Exception getCause() {
            return cause;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RESULT, resultType == null ? "failed" : resultType.getLowercase());
            builder.field(STATUS, status.getStatus());
            builder.field(_ID, roleName);
            if (resultType == null) {
                builder.startObject(ERROR);
                ElasticsearchException.generateThrowableXContent(builder, params, cause);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        public static Item success(String roleName, DocWriteResponse.Result result, RestStatus status) {
            return new Item(roleName, result, status, null);
        }

        public static Item failure(String roleName, Exception cause) {
            return failure(roleName, cause, ExceptionsHelper.status(cause));
        }

        public static Item failure(String roleName, Exception cause, RestStatus status) {
            return new Item(roleName, null, status, cause);
        }

        public String getRoleName() {
            return roleName;
        }

        public boolean isFailed() {
            return cause != null;
        }

        public String getFailureMessage() {
            if (cause != null) {
                return cause.getMessage();
            }
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(roleName);
            resultType.writeTo(out);
            RestStatus.writeTo(out, status);
            out.writeException(cause);
        }
    }

    public List<Item> getItems() {
        return items;
    }

}
