/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

public class ApiKeyAggregationsBuilder {

    public static void verifyAggsBuilder(@Nullable AggregatorFactories.Builder aggsBuilder) {
        if (aggsBuilder == null) {
            return;
        }
        for (PipelineAggregationBuilder pipelineAggregator : aggsBuilder.getPipelineAggregatorFactories()) {
            throw new IllegalArgumentException("Unsupported pipeline aggregation of type [" + pipelineAggregator.getType() + "]");
        }
        for (AggregationBuilder aggregator : aggsBuilder.getAggregatorFactories()) {
            doVerifyAggsBuilder(aggregator);
        }
    }

    private static void doVerifyAggsBuilder(AggregationBuilder aggregationBuilder) {
        // Most of these can be supported without much hassle, but they're not useful for the identified use cases so far
        for (PipelineAggregationBuilder pipelineAggregator : aggregationBuilder.getPipelineAggregations()) {
            throw new IllegalArgumentException("Unsupported pipeline aggregation of type [" + pipelineAggregator.getType() + "]");
        }
        if (aggregationBuilder instanceof TermsAggregationBuilder termsAggregationBuilder) {
            // scripts are not currently supported because it's harder to restrict and rename the doc fields the script has access to
            if (termsAggregationBuilder.script() != null) {
                throw new IllegalArgumentException("Unsupported script value source for [" + aggregationBuilder.getName() + "] agg");
            }
            // the user-facing field names are different from the index mapping field names of API Key docs
            termsAggregationBuilder.field(ApiKeyFieldNameTranslators.translate(termsAggregationBuilder.field()));
        } else if (aggregationBuilder instanceof CompositeAggregationBuilder compositeAggregationBuilder) {
            for (CompositeValuesSourceBuilder<?> valueSource : compositeAggregationBuilder.sources()) {
                if (valueSource.script() != null) {
                    throw new IllegalArgumentException(
                        "Unsupported script value source for ["
                            + valueSource.name()
                            + "] of composite agg ["
                            + compositeAggregationBuilder.getName()
                            + "]"
                    );
                }
                if (valueSource instanceof TermsValuesSourceBuilder == false) {
                    throw new IllegalArgumentException(
                        "Unsupported value source type for ["
                            + valueSource.name()
                            + "] of composite agg ["
                            + compositeAggregationBuilder.getName()
                            + "]."
                            + "Only [terms] value sources are allowed."
                    );
                }
                valueSource.field(ApiKeyFieldNameTranslators.translate(valueSource.field()));
            }
        } else if (aggregationBuilder instanceof DateRangeAggregationBuilder dateRangeAggregationBuilder) {
            if (dateRangeAggregationBuilder.script() != null) {
                throw new IllegalArgumentException("Unsupported script value source for [" + aggregationBuilder.getName() + "] agg");
            }
            // the user-facing field names are different from the index mapping field names of API Key docs
            dateRangeAggregationBuilder.field(ApiKeyFieldNameTranslators.translate(dateRangeAggregationBuilder.field()));
        } else {
            throw new IllegalArgumentException(
                "Unsupported agg [" + aggregationBuilder.getName() + "] of type [" + aggregationBuilder.getType() + "]"
            );
        }
        // check sub-aggs recursively
        for (AggregationBuilder subAggregation : aggregationBuilder.getSubAggregations()) {
            doVerifyAggsBuilder(subAggregation);
        }
    }
}
