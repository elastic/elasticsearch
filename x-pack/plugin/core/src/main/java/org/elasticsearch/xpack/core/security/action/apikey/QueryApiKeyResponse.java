/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.action.ApiKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Response for search API keys.<br>
 * The result contains information about the API keys that were found.
 */
public final class QueryApiKeyResponse extends ActionResponse implements ToXContentObject, Writeable {

    private final long total;
    private final Item[] items;

    public QueryApiKeyResponse(StreamInput in) throws IOException {
        super(in);
        this.total = in.readLong();
        this.items = in.readArray(Item::new, Item[]::new);
    }

    public QueryApiKeyResponse(long total, Collection<Item> items) {
        this.total = total;
        Objects.requireNonNull(items, "items must be provided");
        this.items = items.toArray(new Item[0]);
    }

    public static QueryApiKeyResponse emptyResponse() {
        return new QueryApiKeyResponse(0, Collections.emptyList());
    }

    public long getTotal() {
        return total;
    }

    public Item[] getItems() {
        return items;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("total", total)
            .field("count", items.length)
            .array("api_keys", (Object[]) items);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(total);
        out.writeArray(items);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        QueryApiKeyResponse that = (QueryApiKeyResponse) o;
        return total == that.total && Arrays.equals(items, that.items);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(total);
        result = 31 * result + Arrays.hashCode(items);
        return result;
    }

    @Override
    public String toString() {
        return "QueryApiKeyResponse{" + "total=" + total + ", items=" + Arrays.toString(items) + '}';
    }

    public static class Item implements ToXContentObject, Writeable {
        ApiKey apiKey;
        @Nullable
        Object[] sortValues;

        public Item(ApiKey apiKey, @Nullable Object[] sortValues) {
            this.apiKey = apiKey;
            this.sortValues = sortValues;
        }

        public Item(StreamInput in) throws IOException {
            this.apiKey = new ApiKey(in);
            this.sortValues = in.readOptionalArray(Lucene::readSortValue, Object[]::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            apiKey.writeTo(out);
            out.writeOptionalArray(Lucene::writeSortValue, sortValues);
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
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
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
