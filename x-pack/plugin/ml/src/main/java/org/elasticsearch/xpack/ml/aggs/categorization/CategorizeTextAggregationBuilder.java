/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xpack.core.ml.aggs.categorization.AbstractCategorizeTextAggregationBuilder;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder.REQUIRED_SIZE_FIELD_NAME;

public class CategorizeTextAggregationBuilder extends AbstractCategorizeTextAggregationBuilder<CategorizeTextAggregationBuilder> {

    public static final ObjectParser<CategorizeTextAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        CategorizeTextAggregationBuilder.NAME,
        CategorizeTextAggregationBuilder::new
    );
    static {
        PARSER.declareString(CategorizeTextAggregationBuilder::setFieldName, FIELD_NAME);
        PARSER.declareInt(CategorizeTextAggregationBuilder::setSimilarityThreshold, SIMILARITY_THRESHOLD);
        // The next two are unused, but accepted and ignored to avoid breaking client code
        PARSER.declareInt((p, c) -> {}, MAX_UNIQUE_TOKENS);
        PARSER.declareInt((p, c) -> {}, MAX_MATCHED_TOKENS);
        PARSER.declareField(
            CategorizeTextAggregationBuilder::setCategorizationAnalyzerConfig,
            (p, c) -> CategorizationAnalyzerConfig.buildFromXContentFragment(p, false),
            CATEGORIZATION_ANALYZER,
            ObjectParser.ValueType.OBJECT_OR_STRING
        );
        PARSER.declareStringArray(CategorizeTextAggregationBuilder::setCategorizationFilters, CATEGORIZATION_FILTERS);
        PARSER.declareInt(CategorizeTextAggregationBuilder::shardSize, TermsAggregationBuilder.SHARD_SIZE_FIELD_NAME);
        PARSER.declareLong(CategorizeTextAggregationBuilder::minDocCount, TermsAggregationBuilder.MIN_DOC_COUNT_FIELD_NAME);
        PARSER.declareLong(CategorizeTextAggregationBuilder::shardMinDocCount, TermsAggregationBuilder.SHARD_MIN_DOC_COUNT_FIELD_NAME);
        PARSER.declareInt(CategorizeTextAggregationBuilder::size, REQUIRED_SIZE_FIELD_NAME);
    }

    private CategorizeTextAggregationBuilder(String name) {
        super(name);
    }

    public CategorizeTextAggregationBuilder(String name, String fieldName) {
        super(name, fieldName);
    }

    public CategorizeTextAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    protected CategorizeTextAggregationBuilder(
        CategorizeTextAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected AggregatorFactory doBuild(
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subfactoriesBuilder
    ) throws IOException {
        return new CategorizeTextAggregatorFactory(
            name,
            getFieldName(),
            getSimilarityThreshold(),
            getBucketCountThresholds(),
            getCategorizationAnalyzerConfig(),
            context,
            parent,
            subfactoriesBuilder,
            metadata
        );
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new CategorizeTextAggregationBuilder(this, factoriesBuilder, metadata);
    }
}
