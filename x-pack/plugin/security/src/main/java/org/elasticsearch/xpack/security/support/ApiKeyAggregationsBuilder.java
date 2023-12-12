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
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.xpack.core.security.authc.Authentication;

public class ApiKeyAggregationsBuilder {

    public static void verifyRequested(
        @Nullable AggregatorFactories.Builder aggsBuilder,
        @Nullable Authentication filteringAuthentication
    ) {
        if (aggsBuilder == null) {
            return;
        }
        // Most of these can be supported without much hassle, but they're not useful for the identified use cases so far
        for (PipelineAggregationBuilder pipelineAggregator : aggsBuilder.getPipelineAggregatorFactories()) {
            throw new IllegalArgumentException("Unsupported pipeline aggregation of type [" + pipelineAggregator.getType() + "]");
        }
        for (AggregationBuilder aggregator : aggsBuilder.getAggregatorFactories()) {
            doVerify(aggregator, filteringAuthentication);
        }
    }

    private static void doVerify(AggregationBuilder aggregationBuilder, @Nullable Authentication filteringAuthentication) {
        // Most of these can be supported without much hassle, but they're not useful for the identified use cases so far
        for (PipelineAggregationBuilder pipelineAggregator : aggregationBuilder.getPipelineAggregations()) {
            throw new IllegalArgumentException("Unsupported pipeline aggregation of type [" + pipelineAggregator.getType() + "]");
        }
        if (aggregationBuilder instanceof ValuesSourceAggregationBuilder<?> valuesSourceAggregationBuilder) {
            if (valuesSourceAggregationBuilder instanceof TermsAggregationBuilder == false
                && valuesSourceAggregationBuilder instanceof RangeAggregationBuilder == false
                && valuesSourceAggregationBuilder instanceof DateRangeAggregationBuilder == false
                && valuesSourceAggregationBuilder instanceof MissingAggregationBuilder == false
                && valuesSourceAggregationBuilder instanceof CardinalityAggregationBuilder == false
                && valuesSourceAggregationBuilder instanceof ValueCountAggregationBuilder == false) {
                throw new IllegalArgumentException(
                    "Unsupported API Keys agg [" + aggregationBuilder.getName() + "] of type [" + aggregationBuilder.getType() + "]"
                );
            }
            // scripts are not currently supported because it's harder to restrict and rename the doc fields the script has access to
            if (valuesSourceAggregationBuilder.script() != null) {
                throw new IllegalArgumentException("Unsupported script value source for [" + aggregationBuilder.getName() + "] agg");
            }
            // the user-facing field names are different from the index mapping field names of API Key docs
            valuesSourceAggregationBuilder.field(ApiKeyFieldNameTranslators.translate(valuesSourceAggregationBuilder.field()));
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
        } else if (aggregationBuilder instanceof GlobalAggregationBuilder) {
            // nothing to verify here
        } else if (aggregationBuilder instanceof FilterAggregationBuilder filterAggregationBuilder) {
            // filters the aggregation query to user's allowed API Keys only
            filterAggregationBuilder.filter(ApiKeyBoolQueryBuilder.build(filterAggregationBuilder.getFilter(), filteringAuthentication));
        } else if (aggregationBuilder instanceof FiltersAggregationBuilder filtersAggregationBuilder) {
            // filters the aggregation queries to user's allowed API Keys only
            for (FiltersAggregator.KeyedFilter keyedFilter : filtersAggregationBuilder.filters()) {
                keyedFilter.filter(ApiKeyBoolQueryBuilder.build(keyedFilter.filter(), filteringAuthentication));
            }
        } else {
            throw new IllegalArgumentException(
                "Unsupported API Keys agg [" + aggregationBuilder.getName() + "] of type [" + aggregationBuilder.getType() + "]"
            );
        }
        // check sub-aggs recursively
        for (AggregationBuilder subAggregation : aggregationBuilder.getSubAggregations()) {
            doVerify(subAggregation, filteringAuthentication);
        }
    }
}
