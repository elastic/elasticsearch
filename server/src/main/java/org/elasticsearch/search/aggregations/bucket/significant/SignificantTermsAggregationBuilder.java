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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseFieldRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.MultiBucketAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.JLHScore;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParser;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class SignificantTermsAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource, SignificantTermsAggregationBuilder>
        implements MultiBucketAggregationBuilder {
    public static final String NAME = "significant_terms";

    static final ParseField BACKGROUND_FILTER = new ParseField("background_filter");
    static final ParseField HEURISTIC = new ParseField("significance_heuristic");

    static final TermsAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new TermsAggregator.BucketCountThresholds(
            3, 0, 10, -1);
    static final SignificanceHeuristic DEFAULT_SIGNIFICANCE_HEURISTIC = new JLHScore();

    public static Aggregator.Parser getParser(ParseFieldRegistry<SignificanceHeuristicParser> significanceHeuristicParserRegistry) {
        ObjectParser<SignificantTermsAggregationBuilder, Void> aggregationParser =
                new ObjectParser<>(SignificantTermsAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareAnyFields(aggregationParser, true, true);

        aggregationParser.declareInt(SignificantTermsAggregationBuilder::shardSize, TermsAggregationBuilder.SHARD_SIZE_FIELD_NAME);

        aggregationParser.declareLong(SignificantTermsAggregationBuilder::minDocCount, TermsAggregationBuilder.MIN_DOC_COUNT_FIELD_NAME);

        aggregationParser.declareLong(SignificantTermsAggregationBuilder::shardMinDocCount,
                TermsAggregationBuilder.SHARD_MIN_DOC_COUNT_FIELD_NAME);

        aggregationParser.declareInt(SignificantTermsAggregationBuilder::size, TermsAggregationBuilder.REQUIRED_SIZE_FIELD_NAME);

        aggregationParser.declareString(SignificantTermsAggregationBuilder::executionHint,
                TermsAggregationBuilder.EXECUTION_HINT_FIELD_NAME);

        aggregationParser.declareObject(SignificantTermsAggregationBuilder::backgroundFilter,
                (p, context) -> parseInnerQueryBuilder(p),
                SignificantTermsAggregationBuilder.BACKGROUND_FILTER);

        aggregationParser.declareField((b, v) -> b.includeExclude(IncludeExclude.merge(v, b.includeExclude())),
                IncludeExclude::parseInclude, IncludeExclude.INCLUDE_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING);

        aggregationParser.declareField((b, v) -> b.includeExclude(IncludeExclude.merge(b.includeExclude(), v)),
                IncludeExclude::parseExclude, IncludeExclude.EXCLUDE_FIELD, ObjectParser.ValueType.STRING_ARRAY);

        for (String name : significanceHeuristicParserRegistry.getNames()) {
            aggregationParser.declareObject(SignificantTermsAggregationBuilder::significanceHeuristic,
                    (p, context) -> {
                        SignificanceHeuristicParser significanceHeuristicParser = significanceHeuristicParserRegistry
                                .lookupReturningNullIfNotFound(name, p.getDeprecationHandler());
                        return significanceHeuristicParser.parse(p);
                    },
                    new ParseField(name));
        }
        return (aggregationName, parser) -> aggregationParser.parse(parser,
            new SignificantTermsAggregationBuilder(aggregationName, null), null);
    }

    private IncludeExclude includeExclude = null;
    private String executionHint = null;
    private QueryBuilder filterBuilder = null;
    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(DEFAULT_BUCKET_COUNT_THRESHOLDS);
    private SignificanceHeuristic significanceHeuristic = DEFAULT_SIGNIFICANCE_HEURISTIC;

    public SignificantTermsAggregationBuilder(String name, ValueType valueType) {
        super(name, ValuesSourceType.ANY, valueType);
    }

    /**
     * Read from a Stream.
     */
    public SignificantTermsAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValuesSourceType.ANY);
        bucketCountThresholds = new BucketCountThresholds(in);
        executionHint = in.readOptionalString();
        filterBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        includeExclude = in.readOptionalWriteable(IncludeExclude::new);
        significanceHeuristic = in.readNamedWriteable(SignificanceHeuristic.class);
    }

    protected SignificantTermsAggregationBuilder(SignificantTermsAggregationBuilder clone,
                                                 Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.bucketCountThresholds = new BucketCountThresholds(clone.bucketCountThresholds);
        this.executionHint = clone.executionHint;
        this.filterBuilder = clone.filterBuilder;
        this.includeExclude = clone.includeExclude;
        this.significanceHeuristic = clone.significanceHeuristic;
    }

    @Override
    protected SignificantTermsAggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new SignificantTermsAggregationBuilder(this, factoriesBuilder, metaData);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        bucketCountThresholds.writeTo(out);
        out.writeOptionalString(executionHint);
        out.writeOptionalNamedWriteable(filterBuilder);
        out.writeOptionalWriteable(includeExclude);
        out.writeNamedWriteable(significanceHeuristic);
    }

    @Override
    protected boolean serializeTargetValueType() {
        return true;
    }

    protected TermsAggregator.BucketCountThresholds getBucketCountThresholds() {
        return new TermsAggregator.BucketCountThresholds(bucketCountThresholds);
    }

    public TermsAggregator.BucketCountThresholds bucketCountThresholds() {
        return bucketCountThresholds;
    }

    public SignificantTermsAggregationBuilder bucketCountThresholds(TermsAggregator.BucketCountThresholds bucketCountThresholds) {
        if (bucketCountThresholds == null) {
            throw new IllegalArgumentException("[bucketCountThresholds] must not be null: [" + name + "]");
        }
        this.bucketCountThresholds = bucketCountThresholds;
        return this;
    }

    @Override
    protected AggregationBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (queryRewriteContext.isMultipleClusters()) {
            assert queryRewriteContext.convertToShardContext() == null;
            // We are coordinating a cross-cluster search request across more than one cluster with local reduction on each remote cluster.
            // We need to set shard_size explicitly to make sure that we don't optimize later for a single shard; although we may end up
            // searching on a single shard per cluster, we will reduce buckets coming from multiple shards as we have multiple clusters.
            BucketCountThresholds bucketCountThresholds = suggestShardSize(
                DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize(), this.bucketCountThresholds, false);
            if (bucketCountThresholds != this.bucketCountThresholds) {
                SignificantTermsAggregationBuilder aggregationBuilder = shallowCopy(factoriesBuilder, metaData);
                aggregationBuilder.bucketCountThresholds = bucketCountThresholds;
                return aggregationBuilder;
            }
        }
        return super.doRewrite(queryRewriteContext);
    }

    static BucketCountThresholds suggestShardSize(int defaultShardSize, BucketCountThresholds bucketCountThresholds, boolean singleShard) {
        // The user has not made a shardSize selection. Use default heuristic to avoid any wrong-ranking caused by distributed counting
        // but request double the usual amount. We typically need more than the number of "top" terms requested by other aggregations
        // as the significance algorithm is in less of a position to down-select at shard-level - some of the things we want to find have
        // only one occurrence on each shard and as such are impossible to differentiate from non-significant terms at that early stage.
        if (bucketCountThresholds.getShardSize() == defaultShardSize) {
            int newShardSize = BucketUtils.suggestShardSideQueueSize(2 * bucketCountThresholds.getRequiredSize(), singleShard);
            BucketCountThresholds updatedBucketCountThresholds = new BucketCountThresholds(bucketCountThresholds);
            updatedBucketCountThresholds.setShardSize(newShardSize);
            return updatedBucketCountThresholds;
        }
        return bucketCountThresholds;
    }

    /**
     * Sets the size - indicating how many term buckets should be returned
     * (defaults to 10)
     */
    public SignificantTermsAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        bucketCountThresholds.setRequiredSize(size);
        return this;
    }

    /**
     * Sets the shard_size - indicating the number of term buckets each shard
     * will return to the coordinating node (the node that coordinates the
     * search execution). The higher the shard size is, the more accurate the
     * results are.
     */
    public SignificantTermsAggregationBuilder shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw new IllegalArgumentException(
                    "[shardSize] must be greater than  0. Found [" + shardSize + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    /**
     * Set the minimum document count terms should have in order to appear in
     * the response.
     */
    public SignificantTermsAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                    "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]");
        }
        bucketCountThresholds.setMinDocCount(minDocCount);
        return this;
    }

    /**
     * Set the minimum document count terms should have on the shard in order to
     * appear in the response.
     */
    public SignificantTermsAggregationBuilder shardMinDocCount(long shardMinDocCount) {
        if (shardMinDocCount < 0) {
            throw new IllegalArgumentException(
                    "[shardMinDocCount] must be greater than or equal to 0. Found [" + shardMinDocCount + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
        return this;
    }

    /**
     * Expert: sets an execution hint to the aggregation.
     */
    public SignificantTermsAggregationBuilder executionHint(String executionHint) {
        this.executionHint = executionHint;
        return this;
    }

    /**
     * Expert: gets an execution hint to the aggregation.
     */
    public String executionHint() {
        return executionHint;
    }

    public SignificantTermsAggregationBuilder backgroundFilter(QueryBuilder backgroundFilter) {
        if (backgroundFilter == null) {
            throw new IllegalArgumentException("[backgroundFilter] must not be null: [" + name + "]");
        }
        this.filterBuilder = backgroundFilter;
        return this;
    }

    public QueryBuilder backgroundFilter() {
        return filterBuilder;
    }

    /**
     * Set terms to include and exclude from the aggregation results
     */
    public SignificantTermsAggregationBuilder includeExclude(IncludeExclude includeExclude) {
        this.includeExclude = includeExclude;
        return this;
    }

    /**
     * Get terms to include and exclude from the aggregation results
     */
    public IncludeExclude includeExclude() {
        return includeExclude;
    }

    public SignificantTermsAggregationBuilder significanceHeuristic(SignificanceHeuristic significanceHeuristic) {
        if (significanceHeuristic == null) {
            throw new IllegalArgumentException("[significanceHeuristic] must not be null: [" + name + "]");
        }
        this.significanceHeuristic = significanceHeuristic;
        return this;
    }

    public SignificanceHeuristic significanceHeuristic() {
        return significanceHeuristic;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource, ?> innerBuild(SearchContext context, ValuesSourceConfig<ValuesSource> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        SignificanceHeuristic executionHeuristic = this.significanceHeuristic.rewrite(context);
        return new SignificantTermsAggregatorFactory(name, config, includeExclude, executionHint, filterBuilder,
                bucketCountThresholds, executionHeuristic, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        bucketCountThresholds.toXContent(builder, params);
        if (executionHint != null) {
            builder.field(TermsAggregationBuilder.EXECUTION_HINT_FIELD_NAME.getPreferredName(), executionHint);
        }
        if (filterBuilder != null) {
            builder.field(BACKGROUND_FILTER.getPreferredName(), filterBuilder);
        }
        if (includeExclude != null) {
            includeExclude.toXContent(builder, params);
        }
        significanceHeuristic.toXContent(builder, params);
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(bucketCountThresholds, executionHint, filterBuilder, includeExclude, significanceHeuristic);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        SignificantTermsAggregationBuilder other = (SignificantTermsAggregationBuilder) obj;
        return Objects.equals(bucketCountThresholds, other.bucketCountThresholds)
                && Objects.equals(executionHint, other.executionHint)
                && Objects.equals(filterBuilder, other.filterBuilder)
                && Objects.equals(includeExclude, other.includeExclude)
                && Objects.equals(significanceHeuristic, other.significanceHeuristic);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
