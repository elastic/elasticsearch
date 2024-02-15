/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Response for search API keys.<br>
 * The result contains information about the API keys that were found.
 */
public final class QueryApiKeyResponse extends ActionResponse implements ToXContentObject {

    private final long total;
    private final Item[] items;
    private final @Nullable InternalAggregations aggregations;

    public QueryApiKeyResponse(long total, Collection<Item> items, @Nullable InternalAggregations aggregations) {
        this.total = total;
        Objects.requireNonNull(items, "items must be provided");
        this.items = items.toArray(new Item[0]);
        this.aggregations = aggregations;
    }

    public static QueryApiKeyResponse emptyResponse() {
        return new QueryApiKeyResponse(0, List.of(), null);
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

    public InternalAggregations getAggregations() {
        return aggregations;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("total", total).field("count", items.length).array("api_keys", (Object[]) items);
        if (aggregations != null) {
            aggregations.toXContent(builder, params);
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryApiKeyResponse that = (QueryApiKeyResponse) o;
        return total == that.total && Arrays.equals(items, that.items) && Objects.equals(aggregations, that.aggregations);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(total);
        result = 31 * result + Arrays.hashCode(items);
        result = 31 * result + Objects.hash(aggregations);
        return result;
    }

    @Override
    public String toString() {
        return "QueryApiKeyResponse{total=" + total + ", items=" + Arrays.toString(items) + ", aggs=" + aggregations + "}";
    }

    public static class Item implements ToXContentObject {
        private final ApiKey apiKey;
        @Nullable
        private final Object[] sortValues;

        public Item(ApiKey apiKey, @Nullable Object[] sortValues) {
            this.apiKey = apiKey;
            this.sortValues = sortValues;
        }

        public ApiKey getApiKey() {
            return apiKey;
        }

        public Object[] getSortValues() {
            return sortValues;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            apiKey.innerToXContent(builder, params);
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
            Item item = (Item) o;
            return Objects.equals(apiKey, item.apiKey) && Arrays.equals(sortValues, item.sortValues);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(apiKey);
            result = 31 * result + Arrays.hashCode(sortValues);
            return result;
        }

        @Override
        public String toString() {
            return "Item{" + "apiKey=" + apiKey + ", sortValues=" + Arrays.toString(sortValues) + '}';
        }
    }
}
