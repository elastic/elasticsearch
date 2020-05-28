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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.JLHScore;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class SignificantTermsAggregationBuilder extends ValuesSourceAggregationBuilder<SignificantTermsAggregationBuilder> {
    public static final String NAME = "significant_terms";

    static final ParseField BACKGROUND_FILTER = new ParseField("background_filter");
    static final ParseField HEURISTIC = new ParseField("significance_heuristic");

    static final TermsAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new TermsAggregator.BucketCountThresholds(
            3, 0, 10, -1);
    static final SignificanceHeuristic DEFAULT_SIGNIFICANCE_HEURISTIC = new JLHScore();

    private static final ObjectParser<SignificantTermsAggregationBuilder, Void> PARSER = new ObjectParser<>(
        SignificantTermsAggregationBuilder.NAME,
        SignificanceHeuristic.class, SignificantTermsAggregationBuilder::significanceHeuristic, null);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);

        PARSER.declareInt(SignificantTermsAggregationBuilder::shardSize, TermsAggregationBuilder.SHARD_SIZE_FIELD_NAME);

        PARSER.declareLong(SignificantTermsAggregationBuilder::minDocCount, TermsAggregationBuilder.MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareLong(SignificantTermsAggregationBuilder::shardMinDocCount,
                TermsAggregationBuilder.SHARD_MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareInt(SignificantTermsAggregationBuilder::size, TermsAggregationBuilder.REQUIRED_SIZE_FIELD_NAME);

        PARSER.declareString(SignificantTermsAggregationBuilder::executionHint,
                TermsAggregationBuilder.EXECUTION_HINT_FIELD_NAME);

        PARSER.declareObject(SignificantTermsAggregationBuilder::backgroundFilter,
                (p, context) -> parseInnerQueryBuilder(p),
                SignificantTermsAggregationBuilder.BACKGROUND_FILTER);

        PARSER.declareField((b, v) -> b.includeExclude(IncludeExclude.merge(v, b.includeExclude())),
                IncludeExclude::parseInclude, IncludeExclude.INCLUDE_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING);

        PARSER.declareField((b, v) -> b.includeExclude(IncludeExclude.merge(b.includeExclude(), v)),
                IncludeExclude::parseExclude, IncludeExclude.EXCLUDE_FIELD, ObjectParser.ValueType.STRING_ARRAY);
    }
    public static SignificantTermsAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new SignificantTermsAggregationBuilder(aggregationName), null);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        SignificantTermsAggregatorFactory.registerAggregators(builder);
    }

    private IncludeExclude includeExclude = null;
    private String executionHint = null;
    private QueryBuilder filterBuilder = null;
    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(DEFAULT_BUCKET_COUNT_THRESHOLDS);
    private SignificanceHeuristic significanceHeuristic = DEFAULT_SIGNIFICANCE_HEURISTIC;

    public SignificantTermsAggregationBuilder(String name) {
        super(name);
    }

    /**
     * Read from a Stream.
     */
    public SignificantTermsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        bucketCountThresholds = new BucketCountThresholds(in);
        executionHint = in.readOptionalString();
        filterBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        includeExclude = in.readOptionalWriteable(IncludeExclude::new);
        significanceHeuristic = in.readNamedWriteable(SignificanceHeuristic.class);
    }

    protected SignificantTermsAggregationBuilder(SignificantTermsAggregationBuilder clone,
                                                 AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.bucketCountThresholds = new BucketCountThresholds(clone.bucketCountThresholds);
        this.executionHint = clone.executionHint;
        this.filterBuilder = clone.filterBuilder;
        this.includeExclude = clone.includeExclude;
        this.significanceHeuristic = clone.significanceHeuristic;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    protected SignificantTermsAggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new SignificantTermsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    protected AggregationBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        if (filterBuilder != null) {
            QueryBuilder rewrittenFilter = filterBuilder.rewrite(queryShardContext);
            if (rewrittenFilter != filterBuilder) {
                SignificantTermsAggregationBuilder rewritten = shallowCopy(factoriesBuilder, metadata);
                rewritten.backgroundFilter(rewrittenFilter);
                return rewritten;
            }
        }
        return super.doRewrite(queryShardContext);
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
    protected boolean serializeTargetValueType(Version version) {
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
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                       ValuesSourceConfig config,
                                                       AggregatorFactory parent,
                                                       AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        SignificanceHeuristic executionHeuristic = this.significanceHeuristic.rewrite(queryShardContext);
        return new SignificantTermsAggregatorFactory(name, config, includeExclude, executionHint, filterBuilder,
                bucketCountThresholds, executionHeuristic, queryShardContext, parent, subFactoriesBuilder, metadata);
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
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketCountThresholds, executionHint, filterBuilder, includeExclude, significanceHeuristic);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
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
