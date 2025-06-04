/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BulkRolesResponse extends ActionResponse implements ToXContentObject {

    private final List<Item> items;

    public static class Builder {

        private final List<Item> items = new LinkedList<>();

        public Builder addItem(Item item) {
            items.add(item);
            return this;
        }

        public BulkRolesResponse build() {
            return new BulkRolesResponse(items);
        }
    }

    public BulkRolesResponse(List<Item> items) {
        this.items = items;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        Map<String, List<Item>> itemsByType = items.stream().collect(Collectors.groupingBy(Item::getResultType));

        for (var resultEntry : itemsByType.entrySet()) {
            if (resultEntry.getKey().equals("errors") == false) {
                builder.startArray(resultEntry.getKey());
                for (var item : resultEntry.getValue()) {
                    item.toXContent(builder, params);
                }
                builder.endArray();
            } else {
                builder.startObject("errors");
                builder.field("count", resultEntry.getValue().size());
                builder.startObject("details");
                for (var item : resultEntry.getValue()) {
                    builder.startObject(item.roleName);
                    item.toXContent(builder, params);
                    builder.endObject();
                }
                builder.endObject();
                builder.endObject();
            }
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    public static class Item implements ToXContentObject {
        private final Exception cause;
        private final String roleName;

        private final DocWriteResponse.Result resultType;

        private Item(String roleName, DocWriteResponse.Result resultType, Exception cause) {
            this.roleName = roleName;
            this.resultType = resultType;
            this.cause = cause;
        }

        Item(StreamInput in) throws IOException {
            roleName = in.readString();
            resultType = DocWriteResponse.Result.readFrom(in);
            cause = in.readException();
        }

        public Exception getCause() {
            return cause;
        }

        public String getResultType() {
            return resultType == null ? "errors" : resultType.getLowercase();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (resultType == null) {
                ElasticsearchException.generateThrowableXContent(builder, params, cause);
            } else {
                builder.value(roleName);
            }
            return builder;
        }

        public static Item success(String roleName, DocWriteResponse.Result result) {
            return new Item(roleName, result, null);
        }

        public static Item failure(String roleName, Exception cause) {
            return new Item(roleName, null, cause);
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
    }

    public List<Item> getItems() {
        return items;
    }

}
