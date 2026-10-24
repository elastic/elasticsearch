/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The accounts a {@link QueryServiceAccountRequest} matched. Unlike {@link GetServiceAccountResponse}, which keys
 * each account by its principal, this is a list: a page of a sorted result has an order to preserve, and each item
 * carries the sort values a caller passes back as {@code search_after} to fetch the next page.
 */
public final class QueryServiceAccountResponse extends ActionResponse implements ToXContentObject {

    public static final QueryServiceAccountResponse EMPTY = new QueryServiceAccountResponse(0, List.of());

    private final long total;
    private final List<Item> items;

    public QueryServiceAccountResponse(long total, List<Item> items) {
        this.total = total;
        this.items = List.copyOf(Objects.requireNonNull(items, "items must be provided"));
    }

    public long getTotal() {
        return total;
    }

    public List<Item> getItems() {
        return items;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("total", total).field("count", items.size()).field("service_accounts", items);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final QueryServiceAccountResponse that = (QueryServiceAccountResponse) o;
        return total == that.total && items.equals(that.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, items);
    }

    @Override
    public String toString() {
        return "QueryServiceAccountResponse{total=" + total + ", items=" + items + "}";
    }

    /**
     * One matched account. Rendered with the principal as a {@code username} field, the name the account
     * authenticates under and the name the query API accepts for it, rather than as the key of the object.
     */
    public record Item(ServiceAccountInfo info, @Nullable Object[] sortValues) implements ToXContentObject {

        public Item {
            Objects.requireNonNull(info, "service account info must be provided");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("username", info.principal());
            info.innerToXContent(builder, params);
            if (sortValues != null && sortValues.length > 0) {
                builder.array("_sort", sortValues);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Item that = (Item) o;
            return info.equals(that.info) && Arrays.equals(sortValues, that.sortValues);
        }

        @Override
        public int hashCode() {
            return 31 * info.hashCode() + Arrays.hashCode(sortValues);
        }

        @Override
        public String toString() {
            return "Item{info=" + info + ", sortValues=" + Arrays.toString(sortValues) + "}";
        }
    }
}
