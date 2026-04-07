/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * Response for the query Users API. <br>
 * Model used to serialize information about the Users that were found.
 */
public final class QueryUserResponse extends ActionResponse implements ToXContentObject {

    private final long total;
    private final Item[] items;

    public QueryUserResponse(long total, Collection<Item> items) {
        this.total = total;
        Objects.requireNonNull(items, "items must be provided");
        this.items = items.toArray(new Item[0]);
    }

    public long getTotal() {
        return total;
    }

    public Item[] getItems() {
        return items;
    }

    public int getCount() {
        return items.length;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("total", total).field("count", items.length).array("users", (Object[]) items);
        return builder.endObject();
    }

    @Override
    public String toString() {
        return "QueryUsersResponse{" + "total=" + total + ", items=" + Arrays.toString(items) + '}';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    public record Item(User user, @Nullable Object[] sortValues, @Nullable String profileUid) implements ToXContentObject {

        @Override
        public Object[] sortValues() {
            return sortValues;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            user.innerToXContent(builder);
            if (sortValues != null && sortValues.length > 0) {
                builder.array("_sort", sortValues);
            }
            if (profileUid != null) {
                builder.field("profile_uid", profileUid);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "Item{" + "user=" + user + ", sortValues=" + Arrays.toString(sortValues) + '}';
        }
    }
}
