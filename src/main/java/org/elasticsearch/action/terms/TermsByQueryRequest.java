/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.terms;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.FilterBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to get the values from a specific field for documents matching a specific query.
 * <p/>
 * The request requires the filter source to be set either using {@link #filter(org.elasticsearch.index.query.FilterBuilder)}, or
 * {@link #filter(byte[])}.
 *
 * @see TermsByQueryResponse
 */
public class TermsByQueryRequest extends BroadcastOperationRequest<TermsByQueryRequest> {

    @Nullable
    protected String routing;
    private long nowInMillis;
    private Float minScore;
    @Nullable
    private String preference;
    private BytesReference filterSource;
    private boolean filterSourceUnsafe;
    @Nullable
    private String[] types = Strings.EMPTY_ARRAY;
    private String field;
    private boolean useBloomFilter = false;
    private Double bloomFpp;         // false positive probability
    private Integer bloomExpectedInsertions;
    private Integer bloomHashFunctions;
    private Long maxTermsPerShard;

    TermsByQueryRequest() {
    }

    /**
     * Constructs a new terms by query request against the provided indices. No indices provided means it will run against all indices.
     */
    public TermsByQueryRequest(String... indices) {
        super(indices);
    }

    /**
     * Validates the request
     *
     * @return null if valid, exception otherwise
     */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        return validationException;
    }

    /**
     * Makes the filter source safe before the request is executed.
     */
    @Override
    protected void beforeStart() {
        if (filterSource != null && filterSourceUnsafe) {
            filterSource = filterSource.copyBytesArray();
            filterSourceUnsafe = false;
        }
    }

    /**
     * The minimum score of the documents to include in the terms by query request.
     */
    public Float minScore() {
        return minScore;
    }

    /**
     * The minimum score of the documents to include in the terms by query. Defaults to <tt>null</tt> which means all
     * documents will be included in the terms by query request.
     */
    public TermsByQueryRequest minScore(float minScore) {
        this.minScore = minScore;
        return this;
    }

    /**
     * The field to extract terms from.
     */
    public String field() {
        return field;
    }

    /**
     * The field to extract terms from.
     */
    public TermsByQueryRequest field(String field) {
        this.field = field;
        return this;
    }

    /**
     * The filter source to execute.
     */
    public BytesReference filterSource() {
        return filterSource;
    }

    /**
     * The filter source to execute.
     *
     * @see {@link org.elasticsearch.index.query.FilterBuilders}
     */
    public TermsByQueryRequest filter(FilterBuilder filterBuilder) {
        this.filterSource = filterBuilder.buildAsBytes();
        this.filterSourceUnsafe = false;
        return this;
    }

    /**
     * The filter source to execute.
     */
    public TermsByQueryRequest filter(XContentBuilder builder) {
        this.filterSource = builder.bytes();
        this.filterSourceUnsafe = false;
        return this;
    }

    /**
     * The filter source to execute. It is preferable to use {@link #filter(byte[])}
     */
    public TermsByQueryRequest filter(String filterSource) {
        this.filterSource = new BytesArray(filterSource);
        this.filterSourceUnsafe = false;
        return this;
    }

    /**
     * The filter source to execute.
     */
    public TermsByQueryRequest filter(byte[] filterSource) {
        return filter(filterSource, 0, filterSource.length, false);
    }

    /**
     * The filter source to execute.
     */
    public TermsByQueryRequest filter(byte[] filterSource, int offset, int length, boolean unsafe) {
        return filter(new BytesArray(filterSource, offset, length), unsafe);
    }

    /**
     * The filter source to execute.
     */
    public TermsByQueryRequest filter(BytesReference filterSource, boolean unsafe) {
        this.filterSource = filterSource;
        this.filterSourceUnsafe = unsafe;
        return this;
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public String[] types() {
        return this.types;
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public TermsByQueryRequest types(String... types) {
        this.types = types;
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public String routing() {
        return this.routing;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public TermsByQueryRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * The current time in milliseconds
     */
    public long nowInMillis() {
        return nowInMillis;
    }

    /**
     * Sets the current time in milliseconds
     */
    public TermsByQueryRequest nowInMillis(long nowInMillis) {
        this.nowInMillis = nowInMillis;
        return this;
    }

    /**
     * The routing values to control the shards that the request will be executed on.
     */
    public TermsByQueryRequest routing(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    /**
     * The preference value to control what node the request will be executed on
     */
    public TermsByQueryRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    /**
     * The current preference value
     */
    public String preference() {
        return this.preference;
    }

    /**
     * If the bloom filter should be used.
     */
    public TermsByQueryRequest useBloomFilter(boolean useBloomFilter) {
        this.useBloomFilter = useBloomFilter;
        return this;
    }

    /**
     * If the bloom filter will be used.
     */
    public boolean useBloomFilter() {
        return useBloomFilter;
    }

    /**
     * The bloom filter false positive probability
     */
    public TermsByQueryRequest bloomFpp(double bloomFpp) {
        this.bloomFpp = bloomFpp;
        return this;
    }

    /**
     * The bloom filter false positive probability
     */
    public Double bloomFpp() {
        return bloomFpp;
    }

    /**
     * The expected insertions size for the bloom filter
     */
    public TermsByQueryRequest bloomExpectedInsertions(int bloomExpectedInsertions) {
        this.bloomExpectedInsertions = bloomExpectedInsertions;
        return this;
    }

    /**
     * The expected insertions for the bloom filter
     */
    public Integer bloomExpectedInsertions() {
        return bloomExpectedInsertions;
    }

    /**
     * The number of hash functions for the bloom filter
     */
    public TermsByQueryRequest bloomHashFunctions(int bloomHashFunctions) {
        this.bloomHashFunctions = bloomHashFunctions;
        return this;
    }

    /**
     * The number of hash functions for the bloom filter
     */
    public Integer bloomHashFunctions() {
        return bloomHashFunctions;
    }

    /**
     * The max number of terms to gather per shard
     */
    public TermsByQueryRequest maxTermsPerShard(long maxTermsPerShard) {
        this.maxTermsPerShard = maxTermsPerShard;
        return this;
    }

    /**
     * The max number of terms to gather per shard
     */
    public Long maxTermsPerShard() {
        return maxTermsPerShard;
    }

    /**
     * Deserialize
     *
     * @param in the input
     * @throws IOException
     */
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        if (in.readBoolean()) {
            minScore = in.readFloat();
        }

        routing = in.readOptionalString();
        preference = in.readOptionalString();

        filterSourceUnsafe = false;
        if (in.readBoolean()) {
            filterSource = in.readBytesReference();
        } else {
            filterSource = null;
        }

        if (in.readBoolean()) {
            types = in.readStringArray();
        }

        field = in.readString();
        nowInMillis = in.readVLong();
        useBloomFilter = in.readBoolean();

        if (in.readBoolean()) {
            bloomFpp = in.readDouble();
        }

        if (in.readBoolean()) {
            bloomExpectedInsertions = in.readVInt();
        }

        if (in.readBoolean()) {
            bloomHashFunctions = in.readVInt();
        }

        if (in.readBoolean()) {
            maxTermsPerShard = in.readVLong();
        }
    }

    /**
     * Serialize
     *
     * @param out the output
     * @throws IOException
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (minScore == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeFloat(minScore);
        }

        out.writeOptionalString(routing);
        out.writeOptionalString(preference);

        if (filterSource != null) {
            out.writeBoolean(true);
            out.writeBytesReference(filterSource);
        } else {
            out.writeBoolean(false);
        }

        if (types == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeStringArray(types);
        }

        out.writeString(field);
        out.writeVLong(nowInMillis);
        out.writeBoolean(useBloomFilter);

        if (bloomFpp == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeDouble(bloomFpp);
        }

        if (bloomExpectedInsertions == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(bloomExpectedInsertions);
        }

        if (bloomHashFunctions == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(bloomHashFunctions);
        }

        if (maxTermsPerShard == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVLong(maxTermsPerShard);
        }
    }

    /**
     * String representation of the request
     *
     * @return
     */
    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(filterSource, false);
        } catch (Exception e) {
            // ignore
        }
        return "[" + Arrays.toString(indices) + "]" + Arrays.toString(types) + ", filterSource[" + sSource + "]";
    }
}
