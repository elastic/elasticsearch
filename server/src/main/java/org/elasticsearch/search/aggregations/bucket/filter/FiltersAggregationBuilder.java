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

package org.elasticsearch.search.aggregations.bucket.filter;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.MultiBucketAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class FiltersAggregationBuilder extends AbstractAggregationBuilder<FiltersAggregationBuilder>
        implements MultiBucketAggregationBuilder {
    public static final String NAME = "filters";

    private static final ParseField FILTERS_FIELD = new ParseField("filters");
    private static final ParseField OTHER_BUCKET_FIELD = new ParseField("other_bucket");
    private static final ParseField OTHER_BUCKET_KEY_FIELD = new ParseField("other_bucket_key");

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
    public FiltersAggregationBuilder(String name, KeyedFilter... filters) {
        this(name, Arrays.asList(filters), true);
    }

    private FiltersAggregationBuilder(String name, List<KeyedFilter> filters, boolean keyed) {
        super(name);
        this.filters = new ArrayList<>(filters);
        if (keyed) {
            // internally we want to have a fixed order of filters, regardless of the order of the filters in the request
            Collections.sort(this.filters, (KeyedFilter kf1, KeyedFilter kf2) -> kf1.key().compareTo(kf2.key()));
            this.keyed = true;
        } else {
            this.keyed = false;
        }
    }

    /**
     * @param name
     *            the name of this aggregation
     * @param filters
     *            the filters to use with this aggregation
     */
    public FiltersAggregationBuilder(String name, QueryBuilder... filters) {
        super(name);
        List<KeyedFilter> keyedFilters = new ArrayList<>(filters.length);
        for (int i = 0; i < filters.length; i++) {
            keyedFilters.add(new KeyedFilter(String.valueOf(i), filters[i]));
        }
        this.filters = keyedFilters;
        this.keyed = false;
    }

    /**
     * Read from a stream.
     */
    public FiltersAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        keyed = in.readBoolean();
        int filtersSize = in.readVInt();
        filters = new ArrayList<>(filtersSize);
        if (keyed) {
            for (int i = 0; i < filtersSize; i++) {
                filters.add(new KeyedFilter(in));
            }
        } else {
            for (int i = 0; i < filtersSize; i++) {
                filters.add(new KeyedFilter(String.valueOf(i), in.readNamedWriteable(QueryBuilder.class)));
            }
        }
        otherBucket = in.readBoolean();
        otherBucketKey = in.readString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(keyed);
        out.writeVInt(filters.size());
        if (keyed) {
            for (KeyedFilter keyedFilter : filters) {
                keyedFilter.writeTo(out);
            }
        } else {
            for (KeyedFilter keyedFilter : filters) {
                out.writeNamedWriteable(keyedFilter.filter());
            }
        }
        out.writeBoolean(otherBucket);
        out.writeString(otherBucketKey);
    }

    /**
     * Set whether to include a bucket for documents not matching any filter
     */
    public FiltersAggregationBuilder otherBucket(boolean otherBucket) {
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
     * @return true if this builders filters have a key
     */
    public boolean isKeyed() {
        return this.keyed;
    }

    /**
     * Set the key to use for the bucket for documents not matching any
     * filter.
     */
    public FiltersAggregationBuilder otherBucketKey(String otherBucketKey) {
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
    protected AggregationBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        List<KeyedFilter> rewrittenFilters = new ArrayList<>(filters.size());
        boolean changed = false;
        for (KeyedFilter kf : filters) {
            QueryBuilder result = Rewriteable.rewrite(kf.filter(), queryShardContext);
            rewrittenFilters.add(new KeyedFilter(kf.key(), result));
            if (result != kf.filter()) {
                changed = true;
            }
        }
        if (changed) {
            return new FiltersAggregationBuilder(getName(), rewrittenFilters, this.keyed);
        } else {
            return this;
        }
    }

    @Override
    protected AggregatorFactory<?> doBuild(SearchContext context, AggregatorFactory<?> parent, Builder subFactoriesBuilder)
            throws IOException {
        return new FiltersAggregatorFactory(name, filters, keyed, otherBucket, otherBucketKey, context, parent,
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

    public static FiltersAggregationBuilder parse(String aggregationName, XContentParser parser)
            throws IOException {

        List<FiltersAggregator.KeyedFilter> keyedFilters = null;
        List<QueryBuilder> nonKeyedFilters = null;

        XContentParser.Token token = null;
        String currentFieldName = null;
        String otherBucketKey = null;
        Boolean otherBucket = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (OTHER_BUCKET_FIELD.match(currentFieldName)) {
                    otherBucket = parser.booleanValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (OTHER_BUCKET_KEY_FIELD.match(currentFieldName)) {
                    otherBucketKey = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (FILTERS_FIELD.match(currentFieldName)) {
                    keyedFilters = new ArrayList<>();
                    String key = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            key = parser.currentName();
                        } else {
                            QueryBuilder filter = parseInnerQueryBuilder(parser);
                            keyedFilters.add(new FiltersAggregator.KeyedFilter(key, filter));
                        }
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (FILTERS_FIELD.match(currentFieldName)) {
                    nonKeyedFilters = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        QueryBuilder filter = parseInnerQueryBuilder(parser);
                        nonKeyedFilters.add(filter);
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
            }
        }

        if (otherBucket == null && otherBucketKey != null) {
            // automatically enable the other bucket if a key is set, as per the doc
            otherBucket = true;
        }

        FiltersAggregationBuilder factory;
        if (keyedFilters != null) {
            factory = new FiltersAggregationBuilder(aggregationName,
                    keyedFilters.toArray(new FiltersAggregator.KeyedFilter[keyedFilters.size()]));
        } else {
            factory = new FiltersAggregationBuilder(aggregationName,
                    nonKeyedFilters.toArray(new QueryBuilder[nonKeyedFilters.size()]));
        }
        if (otherBucket != null) {
            factory.otherBucket(otherBucket);
        }
        if (otherBucketKey != null) {
            factory.otherBucketKey(otherBucketKey);
        }
        return factory;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(filters, keyed, otherBucket, otherBucketKey);
    }

    @Override
    protected boolean doEquals(Object obj) {
        FiltersAggregationBuilder other = (FiltersAggregationBuilder) obj;
        return Objects.equals(filters, other.filters)
                && Objects.equals(keyed, other.keyed)
                && Objects.equals(otherBucket, other.otherBucket)
                && Objects.equals(otherBucketKey, other.otherBucketKey);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
