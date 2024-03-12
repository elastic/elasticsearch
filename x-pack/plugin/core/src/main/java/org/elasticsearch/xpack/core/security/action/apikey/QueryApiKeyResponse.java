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

    public static final QueryApiKeyResponse EMPTY = new QueryApiKeyResponse(0, List.of(), List.of(), List.of(), null);

    private final long total;
    private final ApiKey[] foundApiKeysInfo;
    @Nullable
    private final Object[][] sortValues;
    @Nullable
    private final String[] ownerProfileUids;
    private final @Nullable InternalAggregations aggregations;

    public QueryApiKeyResponse(
        long total,
        Collection<ApiKey> foundApiKeysInfo,
        @Nullable Collection<Object[]> sortValues,
        @Nullable Collection<String> ownerProfileUids,
        @Nullable InternalAggregations aggregations
    ) {
        this.total = total;
        Objects.requireNonNull(foundApiKeysInfo, "found_api_keys_info must be provided");
        this.foundApiKeysInfo = foundApiKeysInfo.toArray(new ApiKey[0]);
        if (sortValues != null) {
            if (foundApiKeysInfo.size() != sortValues.size()) {
                throw new IllegalArgumentException("Every api key info must be associated to an (optional) sort value");
            }
            this.sortValues = sortValues.toArray(Object[][]::new);
        } else {
            this.sortValues = null;
        }
        if (ownerProfileUids != null) {
            if (foundApiKeysInfo.size() != ownerProfileUids.size()) {
                throw new IllegalArgumentException("Every api key info must be associated to an (optional) owner profile id");
            }
            this.ownerProfileUids = ownerProfileUids.toArray(new String[0]);
        } else {
            this.ownerProfileUids = null;
        }
        this.aggregations = aggregations;
    }

    public long getTotal() {
        return total;
    }

    public ApiKey[] getApiKeyInfos() {
        return foundApiKeysInfo;
    }

    public int getCount() {
        assert sortValues == null || sortValues.length == foundApiKeysInfo.length;
        assert ownerProfileUids == null || ownerProfileUids.length == foundApiKeysInfo.length;
        return foundApiKeysInfo.length;
    }

    public InternalAggregations getAggregations() {
        return aggregations;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("total", total);
        builder.field("count", foundApiKeysInfo.length);
        builder.startArray("api_keys");
        for (int i = 0; i < foundApiKeysInfo.length; i++) {
            builder.startObject();
            foundApiKeysInfo[i].innerToXContent(builder, params);
            if (sortValues != null && sortValues[i] != null && sortValues[i].length > 0) {
                builder.array("_sort", sortValues[i]);
            }
            if (ownerProfileUids != null && ownerProfileUids[i] != null) {
                builder.field("profile_uid", ownerProfileUids[i]);
            }
            builder.endObject();
        }
        builder.endArray();
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
            && Arrays.equals(foundApiKeysInfo, that.foundApiKeysInfo)
            && Arrays.equals(ownerProfileUids, that.ownerProfileUids)
            && Arrays.deepEquals(sortValues, that.sortValues)
            && Objects.equals(aggregations, that.aggregations);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(total);
        result = 31 * result + Arrays.hashCode(foundApiKeysInfo);
        result = 31 * result + Arrays.hashCode(ownerProfileUids);
        result = 31 * result + Arrays.deepHashCode(sortValues);
        result = 31 * result + Objects.hash(aggregations);
        return result;
    }

    @Override
    public String toString() {
        return "QueryApiKeyResponse{total="
            + total
            + ", foundApiKeysInfo="
            + Arrays.toString(foundApiKeysInfo)
            + ", sortValues="
            + Arrays.toString(sortValues)
            + ", ownerProfileUids="
            + Arrays.toString(ownerProfileUids)
            + ", aggs="
            + aggregations
            + "}";
    }
}
