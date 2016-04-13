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

package org.elasticsearch.search.aggregations.bucket.filters;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregatorBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class FiltersAggregatorBuilder extends AggregatorBuilder<FiltersAggregatorBuilder> {

    static final FiltersAggregatorBuilder PROTOTYPE = new FiltersAggregatorBuilder("", new MatchAllQueryBuilder());

    private final List<KeyedFilter> filters;
    private final boolean keyed;
    private boolean otherBucket = false;
    private String otherBucketKey = "_other_";

    /**
     * @param name
     *            the name of this aggregation
     * @param filters
     *            the KeyedFilters to use with this aggregation.
     */
    public FiltersAggregatorBuilder(String name, KeyedFilter... filters) {
        this(name, Arrays.asList(filters));
    }

    private FiltersAggregatorBuilder(String name, List<KeyedFilter> filters) {
        super(name, InternalFilters.TYPE);
        // internally we want to have a fixed order of filters, regardless of the order of the filters in the request
        this.filters = new ArrayList<>(filters);
        Collections.sort(this.filters, (KeyedFilter kf1, KeyedFilter kf2) -> kf1.key().compareTo(kf2.key()));
        this.keyed = true;
    }

    /**
     * @param name
     *            the name of this aggregation
     * @param filters
     *            the filters to use with this aggregation
     */
    public FiltersAggregatorBuilder(String name, QueryBuilder<?>... filters) {
        super(name, InternalFilters.TYPE);
        List<KeyedFilter> keyedFilters = new ArrayList<>(filters.length);
        for (int i = 0; i < filters.length; i++) {
            keyedFilters.add(new KeyedFilter(String.valueOf(i), filters[i]));
        }
        this.filters = keyedFilters;
        this.keyed = false;
    }

    /**
     * Set whether to include a bucket for documents not matching any filter
     */
    public FiltersAggregatorBuilder otherBucket(boolean otherBucket) {
        this.otherBucket = otherBucket;
        return this;
    }

    /**
     * Get whether to include a bucket for documents not matching any filter
     */
    public boolean otherBucket() {
        return otherBucket;
    }

    /**
     * Get the filters. This will be an unmodifiable list
     */
    public List<KeyedFilter> filters() {
        return Collections.unmodifiableList(this.filters);
    }

    /**
     * Set the key to use for the bucket for documents not matching any
     * filter.
     */
    public FiltersAggregatorBuilder otherBucketKey(String otherBucketKey) {
        if (otherBucketKey == null) {
            throw new IllegalArgumentException("[otherBucketKey] must not be null: [" + name + "]");
        }
        this.otherBucketKey = otherBucketKey;
        return this;
    }

    /**
     * Get the key to use for the bucket for documents not matching any
     * filter.
     */
    public String otherBucketKey() {
        return otherBucketKey;
    }

    @Override
    protected AggregatorFactory<?> doBuild(AggregationContext context, AggregatorFactory<?> parent, Builder subFactoriesBuilder)
            throws IOException {
        return new FiltersAggregatorFactory(name, type, filters, keyed, otherBucket, otherBucketKey, context, parent,
                subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (keyed) {
            builder.startObject(FiltersAggregator.FILTERS_FIELD.getPreferredName());
            for (KeyedFilter keyedFilter : filters) {
                builder.field(keyedFilter.key(), keyedFilter.filter());
            }
            builder.endObject();
        } else {
            builder.startArray(FiltersAggregator.FILTERS_FIELD.getPreferredName());
            for (KeyedFilter keyedFilter : filters) {
                builder.value(keyedFilter.filter());
            }
            builder.endArray();
        }
        builder.field(FiltersAggregator.OTHER_BUCKET_FIELD.getPreferredName(), otherBucket);
        builder.field(FiltersAggregator.OTHER_BUCKET_KEY_FIELD.getPreferredName(), otherBucketKey);
        builder.endObject();
        return builder;
    }

    @Override
    protected FiltersAggregatorBuilder doReadFrom(String name, StreamInput in) throws IOException {
        FiltersAggregatorBuilder factory;
        if (in.readBoolean()) {
            int size = in.readVInt();
            List<KeyedFilter> filters = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                filters.add(KeyedFilter.PROTOTYPE.readFrom(in));
            }
            factory = new FiltersAggregatorBuilder(name, filters);
        } else {
            int size = in.readVInt();
            QueryBuilder<?>[] filters = new QueryBuilder<?>[size];
            for (int i = 0; i < size; i++) {
                filters[i] = in.readQuery();
            }
            factory = new FiltersAggregatorBuilder(name, filters);
        }
        factory.otherBucket = in.readBoolean();
        factory.otherBucketKey = in.readString();
        return factory;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(keyed);
        if (keyed) {
            out.writeVInt(filters.size());
            for (KeyedFilter keyedFilter : filters) {
                keyedFilter.writeTo(out);
            }
        } else {
            out.writeVInt(filters.size());
            for (KeyedFilter keyedFilter : filters) {
                out.writeQuery(keyedFilter.filter());
            }
        }
        out.writeBoolean(otherBucket);
        out.writeString(otherBucketKey);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(filters, keyed, otherBucket, otherBucketKey);
    }

    @Override
    protected boolean doEquals(Object obj) {
        FiltersAggregatorBuilder other = (FiltersAggregatorBuilder) obj;
        return Objects.equals(filters, other.filters)
                && Objects.equals(keyed, other.keyed)
                && Objects.equals(otherBucket, other.otherBucket)
                && Objects.equals(otherBucketKey, other.otherBucketKey);
    }
}