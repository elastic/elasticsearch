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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Response for search API keys.<br>
 * The result contains information about the API keys that were found.
 */
public final class QueryApiKeyResponse extends ActionResponse implements ToXContentObject {

    public static final QueryApiKeyResponse EMPTY = new QueryApiKeyResponse(0, List.of(), List.of(), null, null);

    private final long total;
    private final List<Item> foundApiKeyInfoList;
    private final @Nullable InternalAggregations aggregations;

    public QueryApiKeyResponse(
        long total,
        Collection<ApiKey> foundApiKeysInfos,
        Collection<Object[]> sortValues,
        @Nullable Collection<String> ownerProfileUids,
        @Nullable InternalAggregations aggregations
    ) {
        this.total = total;
        Objects.requireNonNull(foundApiKeysInfos, "found_api_keys_infos must be provided");
        Objects.requireNonNull(sortValues, "sort_values must be provided");
        if (foundApiKeysInfos.size() != sortValues.size()) {
            throw new IllegalStateException("Each api key info must be associated to a (nullable) sort value");
        }
        if (ownerProfileUids != null && foundApiKeysInfos.size() != ownerProfileUids.size()) {
            throw new IllegalStateException("Each api key info must be associated to a (nullable) owner profile uid");
        }
        int size = foundApiKeysInfos.size();
        this.foundApiKeyInfoList = new ArrayList<>(size);
        Iterator<ApiKey> apiKeyIterator = foundApiKeysInfos.iterator();
        Iterator<Object[]> sortValueIterator = sortValues.iterator();
        Iterator<String> profileUidIterator = ownerProfileUids != null ? ownerProfileUids.iterator() : null;
        while (apiKeyIterator.hasNext()) {
            if (false == sortValueIterator.hasNext()) {
                throw new IllegalStateException("Each api key info must be associated to a (nullable) sort value");
            }
            if (profileUidIterator != null && false == profileUidIterator.hasNext()) {
                throw new IllegalStateException("Each api key info must be associated to a (nullable) owner profile uid");
            }
            this.foundApiKeyInfoList.add(
                new Item(apiKeyIterator.next(), sortValueIterator.next(), profileUidIterator != null ? profileUidIterator.next() : null)
            );
        }
        this.aggregations = aggregations;
    }

    public long getTotal() {
        return total;
    }

    public List<Item> getApiKeyInfoList() {
        return foundApiKeyInfoList;
    }

    public InternalAggregations getAggregations() {
        return aggregations;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("total", total).field("count", foundApiKeyInfoList.size()).field("api_keys", foundApiKeyInfoList);
        if (aggregations != null) {
            aggregations.toXContent(builder, params);
        }
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
        QueryApiKeyResponse that = (QueryApiKeyResponse) o;
        return total == that.total
            && Objects.equals(foundApiKeyInfoList, that.foundApiKeyInfoList)
            && Objects.equals(aggregations, that.aggregations);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(total);
        result = 31 * result + Objects.hash(foundApiKeyInfoList);
        result = 31 * result + Objects.hash(aggregations);
        return result;
    }

    @Override
    public String toString() {
        return "QueryApiKeyResponse{total=" + total + ", items=" + foundApiKeyInfoList + ", aggs=" + aggregations + "}";
    }

    public record Item(ApiKey apiKeyInfo, @Nullable Object[] sortValues, @Nullable String ownerProfileUid) implements ToXContentObject {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            apiKeyInfo.innerToXContent(builder, params);
            if (sortValues != null && sortValues.length > 0) {
                builder.array("_sort", sortValues);
            }
            if (ownerProfileUid != null) {
                builder.field("profile_uid", ownerProfileUid);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "Item{apiKeyInfo="
                + apiKeyInfo
                + ", sortValues="
                + Arrays.toString(sortValues)
                + ", ownerProfileUid="
                + ownerProfileUid
                + '}';
        }
    }
}
