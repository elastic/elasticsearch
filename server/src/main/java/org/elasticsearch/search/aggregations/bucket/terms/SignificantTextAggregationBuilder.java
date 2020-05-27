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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SignificantTextAggregationBuilder extends AbstractAggregationBuilder<SignificantTextAggregationBuilder> {
    public static final String NAME = "significant_text";

    static final ParseField FIELD_NAME = new ParseField("field");
    static final ParseField SOURCE_FIELDS_NAME = new ParseField("source_fields");
    static final ParseField FILTER_DUPLICATE_TEXT_FIELD_NAME = new ParseField(
            "filter_duplicate_text");

    static final TermsAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS =
            SignificantTermsAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS;
    static final SignificanceHeuristic DEFAULT_SIGNIFICANCE_HEURISTIC = SignificantTermsAggregationBuilder.DEFAULT_SIGNIFICANCE_HEURISTIC;

    private String fieldName = null;
    private String [] sourceFieldNames = null;
    private boolean filterDuplicateText = false;
    private IncludeExclude includeExclude = null;
    private QueryBuilder filterBuilder = null;
    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(
            DEFAULT_BUCKET_COUNT_THRESHOLDS);
    private SignificanceHeuristic significanceHeuristic = DEFAULT_SIGNIFICANCE_HEURISTIC;

    private static final ObjectParser<SignificantTextAggregationBuilder, Void> PARSER = new ObjectParser<>(
                SignificantTextAggregationBuilder.NAME,
                SignificanceHeuristic.class, SignificantTextAggregationBuilder::significanceHeuristic, null);
    static {
        PARSER.declareInt(SignificantTextAggregationBuilder::shardSize,
                TermsAggregationBuilder.SHARD_SIZE_FIELD_NAME);

        PARSER.declareLong(SignificantTextAggregationBuilder::minDocCount,
                TermsAggregationBuilder.MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareLong(SignificantTextAggregationBuilder::shardMinDocCount,
                TermsAggregationBuilder.SHARD_MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareInt(SignificantTextAggregationBuilder::size,
                TermsAggregationBuilder.REQUIRED_SIZE_FIELD_NAME);

        PARSER.declareString(SignificantTextAggregationBuilder::fieldName, FIELD_NAME);

        PARSER.declareStringArray(SignificantTextAggregationBuilder::sourceFieldNames, SOURCE_FIELDS_NAME);


        PARSER.declareBoolean(SignificantTextAggregationBuilder::filterDuplicateText,
                FILTER_DUPLICATE_TEXT_FIELD_NAME);

        PARSER.declareObject(SignificantTextAggregationBuilder::backgroundFilter,
                (p, context) -> AbstractQueryBuilder.parseInnerQueryBuilder(p),
                SignificantTermsAggregationBuilder.BACKGROUND_FILTER);

        PARSER.declareField((b, v) -> b.includeExclude(IncludeExclude.merge(v, b.includeExclude())),
                IncludeExclude::parseInclude, IncludeExclude.INCLUDE_FIELD,
                ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING);

        PARSER.declareField((b, v) -> b.includeExclude(IncludeExclude.merge(b.includeExclude(), v)),
                IncludeExclude::parseExclude, IncludeExclude.EXCLUDE_FIELD,
                ObjectParser.ValueType.STRING_ARRAY);
    }
    public static SignificantTextAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new SignificantTextAggregationBuilder(aggregationName, null), null);
    }

    protected SignificantTextAggregationBuilder(SignificantTextAggregationBuilder clone,
                                                Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.bucketCountThresholds = new BucketCountThresholds(clone.bucketCountThresholds);
        this.fieldName = clone.fieldName;
        this.filterBuilder = clone.filterBuilder;
        this.filterDuplicateText = clone.filterDuplicateText;
        this.includeExclude = clone.includeExclude;
        this.significanceHeuristic = clone.significanceHeuristic;
        this.sourceFieldNames = clone.sourceFieldNames;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new SignificantTextAggregationBuilder(this, factoriesBuilder, metadata);
    }

    protected TermsAggregator.BucketCountThresholds getBucketCountThresholds() {
        return new TermsAggregator.BucketCountThresholds(bucketCountThresholds);
    }

    public TermsAggregator.BucketCountThresholds bucketCountThresholds() {
        return bucketCountThresholds;
    }


    @Override
    public SignificantTextAggregationBuilder subAggregations(Builder subFactories) {
        throw new AggregationInitializationException("Aggregator [" + name + "] of type ["
                + getType() + "] cannot accept sub-aggregations");
    }

    @Override
    public SignificantTextAggregationBuilder subAggregation(AggregationBuilder aggregation) {
        throw new AggregationInitializationException("Aggregator [" + name + "] of type ["
                + getType() + "] cannot accept sub-aggregations");
    }

    public SignificantTextAggregationBuilder bucketCountThresholds(
            TermsAggregator.BucketCountThresholds bucketCountThresholds) {
        if (bucketCountThresholds == null) {
            throw new IllegalArgumentException(
                    "[bucketCountThresholds] must not be null: [" + name + "]");
        }
        this.bucketCountThresholds = bucketCountThresholds;
        return this;
    }

    /**
     * Sets the size - indicating how many term buckets should be returned
     * (defaults to 10)
     */
    public SignificantTextAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException(
                    "[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
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
    public SignificantTextAggregationBuilder shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw new IllegalArgumentException("[shardSize] must be greater than  0. Found ["
                    + shardSize + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    /**
     * Sets the name of the text field that will be the subject of this
     * aggregation.
     */
    public SignificantTextAggregationBuilder fieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }


    /**
     * Selects the fields to load from _source JSON and analyze.
     * If none are specified, the indexed "fieldName" value is assumed
     * to also be the name of the JSON field holding the value
     */
    public SignificantTextAggregationBuilder sourceFieldNames(List<String> names) {
        this.sourceFieldNames = names.toArray(new String [names.size()]);
        return this;
    }


    /**
     * Control if duplicate paragraphs of text should try be filtered from the
     * statistical text analysis. Can improve results but slows down analysis.
     * Default is false.
     */
    public SignificantTextAggregationBuilder filterDuplicateText(boolean filterDuplicateText) {
        this.filterDuplicateText = filterDuplicateText;
        return this;
    }

    /**
     * Set the minimum document count terms should have in order to appear in
     * the response.
     */
    public SignificantTextAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                    "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount
                            + "] in [" + name + "]");
        }
        bucketCountThresholds.setMinDocCount(minDocCount);
        return this;
    }

    /**
     * Set the minimum document count terms should have on the shard in order to
     * appear in the response.
     */
    public SignificantTextAggregationBuilder shardMinDocCount(long shardMinDocCount) {
        if (shardMinDocCount < 0) {
            throw new IllegalArgumentException(
                    "[shardMinDocCount] must be greater than or equal to 0. Found ["
                            + shardMinDocCount + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
        return this;
    }

    public SignificantTextAggregationBuilder backgroundFilter(QueryBuilder backgroundFilter) {
        if (backgroundFilter == null) {
            throw new IllegalArgumentException(
                    "[backgroundFilter] must not be null: [" + name + "]");
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
    public SignificantTextAggregationBuilder includeExclude(IncludeExclude includeExclude) {
        this.includeExclude = includeExclude;
        return this;
    }

    /**
     * Get terms to include and exclude from the aggregation results
     */
    public IncludeExclude includeExclude() {
        return includeExclude;
    }

    public SignificantTextAggregationBuilder significanceHeuristic(
            SignificanceHeuristic significanceHeuristic) {
        if (significanceHeuristic == null) {
            throw new IllegalArgumentException(
                    "[significanceHeuristic] must not be null: [" + name + "]");
        }
        this.significanceHeuristic = significanceHeuristic;
        return this;
    }

    public SignificanceHeuristic significanceHeuristic() {
        return significanceHeuristic;
    }

    /**
     * @param name
     *            the name of this aggregation
     * @param fieldName
     *            the name of the text field that will be the subject of this
     *            aggregation
     *
     */
    public SignificantTextAggregationBuilder(String name, String fieldName) {
        super(name);
        this.fieldName = fieldName;
    }

    /**
     * Read from a stream.
     */
    public SignificantTextAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        filterDuplicateText = in.readBoolean();
        bucketCountThresholds = new BucketCountThresholds(in);
        filterBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        includeExclude = in.readOptionalWriteable(IncludeExclude::new);
        significanceHeuristic = in.readNamedWriteable(SignificanceHeuristic.class);
        sourceFieldNames = in.readOptionalStringArray();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeBoolean(filterDuplicateText);
        bucketCountThresholds.writeTo(out);
        out.writeOptionalNamedWriteable(filterBuilder);
        out.writeOptionalWriteable(includeExclude);
        out.writeNamedWriteable(significanceHeuristic);
        out.writeOptionalStringArray(sourceFieldNames);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected AggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent,
                                        Builder subFactoriesBuilder) throws IOException {
        SignificanceHeuristic executionHeuristic = this.significanceHeuristic.rewrite(queryShardContext);

        return new SignificantTextAggregatorFactory(name, includeExclude, filterBuilder,
                bucketCountThresholds, executionHeuristic, queryShardContext, parent, subFactoriesBuilder,
                fieldName, sourceFieldNames, filterDuplicateText, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params)
            throws IOException {
        builder.startObject();
        bucketCountThresholds.toXContent(builder, params);
        if (fieldName != null) {
            builder.field(FIELD_NAME.getPreferredName(), fieldName);
        }
        if (sourceFieldNames != null) {
            builder.array(SOURCE_FIELDS_NAME.getPreferredName(), sourceFieldNames);
        }

        if (filterDuplicateText) {
            builder.field(FILTER_DUPLICATE_TEXT_FIELD_NAME.getPreferredName(), filterDuplicateText);
        }
        if (filterBuilder != null) {
            builder.field(SignificantTermsAggregationBuilder.BACKGROUND_FILTER.getPreferredName(),
                    filterBuilder);
        }
        if (includeExclude != null) {
            includeExclude.toXContent(builder, params);
        }
        significanceHeuristic.toXContent(builder, params);

        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketCountThresholds, fieldName,
            filterDuplicateText, filterBuilder,
            includeExclude, significanceHeuristic, Arrays.hashCode(sourceFieldNames));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        SignificantTextAggregationBuilder other = (SignificantTextAggregationBuilder) obj;
        return Objects.equals(bucketCountThresholds, other.bucketCountThresholds)
                && Objects.equals(fieldName, other.fieldName)
                && Arrays.equals(sourceFieldNames, other.sourceFieldNames)
                && filterDuplicateText == other.filterDuplicateText
                && Objects.equals(filterBuilder, other.filterBuilder)
                && Objects.equals(includeExclude, other.includeExclude)
                && Objects.equals(significanceHeuristic, other.significanceHeuristic);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
