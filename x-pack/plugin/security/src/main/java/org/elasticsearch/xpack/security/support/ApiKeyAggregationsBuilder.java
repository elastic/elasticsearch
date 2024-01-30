/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
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

import java.util.function.Consumer;

public class ApiKeyAggregationsBuilder {

    public static AggregatorFactories.Builder process(
        @Nullable AggregatorFactories.Builder aggsBuilder,
        @Nullable Consumer<String> fieldNameVisitor,
        @Nullable Authentication filteringAuthentication
    ) {
        if (aggsBuilder == null) {
            return null;
        }
        // Most of these can be supported without much hassle, but they're not useful for the identified use cases so far
        for (PipelineAggregationBuilder pipelineAggregator : aggsBuilder.getPipelineAggregatorFactories()) {
            throw new IllegalArgumentException("Unsupported pipeline aggregation of type [" + pipelineAggregator.getType() + "]");
        }
        AggregatorFactories.Builder copiedAggsBuilder = AggregatorFactories.builder();
        for (AggregationBuilder aggregationBuilder : aggsBuilder.getAggregatorFactories()) {
            copiedAggsBuilder.addAggregator(
                doProcess(aggregationBuilder, fieldNameVisitor != null ? fieldNameVisitor : ignored -> {}, filteringAuthentication)
            );
        }
        return copiedAggsBuilder;
    }

    private static AggregationBuilder doProcess(
        AggregationBuilder aggregationBuilder,
        Consumer<String> fieldNameVisitor,
        @Nullable Authentication filteringAuthentication
    ) {
        // Most of these can be supported without much hassle, but they're not useful for the identified use cases so far
        for (PipelineAggregationBuilder pipelineAggregator : aggregationBuilder.getPipelineAggregations()) {
            throw new IllegalArgumentException("Unsupported pipeline aggregation of type [" + pipelineAggregator.getType() + "]");
        }
        AggregatorFactories.Builder subAggregations = new AggregatorFactories.Builder();
        // check sub-aggs recursively
        for (AggregationBuilder subAggregation : aggregationBuilder.getSubAggregations()) {
            subAggregations.addAggregator(doProcess(subAggregation, fieldNameVisitor, filteringAuthentication));
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
            ValuesSourceAggregationBuilder<?> copiedValuesSAggregationBuilder = (ValuesSourceAggregationBuilder<
                ?>) valuesSourceAggregationBuilder.shallowCopy(subAggregations, valuesSourceAggregationBuilder.getMetadata());
            // the user-facing field names are different from the index mapping field names of API Key docs
            String translatedFieldName = ApiKeyFieldNameTranslators.translate(valuesSourceAggregationBuilder.field());
            copiedValuesSAggregationBuilder.field(translatedFieldName);
            fieldNameVisitor.accept(translatedFieldName);
            return copiedValuesSAggregationBuilder;
        } else if (aggregationBuilder instanceof CompositeAggregationBuilder compositeAggregationBuilder) {
            CompositeAggregationBuilder copiedCompositeAggregationBuilder = (CompositeAggregationBuilder) compositeAggregationBuilder
                .shallowCopy(subAggregations, compositeAggregationBuilder.getMetadata());
            for (CompositeValuesSourceBuilder<?> valueSource : copiedCompositeAggregationBuilder.sources()) {
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
                String translatedFieldName = ApiKeyFieldNameTranslators.translate(valueSource.field());
                valueSource.field(translatedFieldName);
                fieldNameVisitor.accept(translatedFieldName);
            }
            return copiedCompositeAggregationBuilder;
        } else if (aggregationBuilder instanceof GlobalAggregationBuilder globalAggregationBuilder) {
            return globalAggregationBuilder.shallowCopy(subAggregations, globalAggregationBuilder.getMetadata());
        } else if (aggregationBuilder instanceof FilterAggregationBuilder filterAggregationBuilder) {
            // filters the aggregation query to user's allowed API Keys only
            FilterAggregationBuilder copiedFilterAggregationBuilder = new FilterAggregationBuilder(
                filterAggregationBuilder.getName(),
                ApiKeyBoolQueryBuilder.build(filterAggregationBuilder.getFilter(), fieldNameVisitor, filteringAuthentication)
            );
            copiedFilterAggregationBuilder.subAggregations(subAggregations).setMetadata(copiedFilterAggregationBuilder.getMetadata());
            return copiedFilterAggregationBuilder;
        } else if (aggregationBuilder instanceof FiltersAggregationBuilder filtersAggregationBuilder) {
            // filters the aggregation's bucket queries to user's allowed API Keys only
            QueryBuilder[] filterQueryBuilders = new QueryBuilder[filtersAggregationBuilder.filters().size()];
            {
                int i = 0;
                for (FiltersAggregator.KeyedFilter keyedFilter : filtersAggregationBuilder.filters()) {
                    filterQueryBuilders[i++] = ApiKeyBoolQueryBuilder.build(
                        keyedFilter.filter(),
                        fieldNameVisitor,
                        filteringAuthentication
                    );
                }
            }
            final FiltersAggregationBuilder copiedFiltersAggregationBuilder;
            if (filtersAggregationBuilder.isKeyed()) {
                FiltersAggregator.KeyedFilter[] keyedFilters = new FiltersAggregator.KeyedFilter[filterQueryBuilders.length];
                for (int i = 0; i < keyedFilters.length; i++) {
                    keyedFilters[i] = new FiltersAggregator.KeyedFilter(
                        filtersAggregationBuilder.filters().get(i).key(),
                        filterQueryBuilders[i]
                    );
                }
                copiedFiltersAggregationBuilder = new FiltersAggregationBuilder(filtersAggregationBuilder.getName(), keyedFilters);
            } else {
                copiedFiltersAggregationBuilder = new FiltersAggregationBuilder(filtersAggregationBuilder.getName(), filterQueryBuilders);
            }
            copiedFiltersAggregationBuilder.otherBucket(filtersAggregationBuilder.otherBucket())
                .otherBucketKey(filtersAggregationBuilder.otherBucketKey())
                .keyedBucket(filtersAggregationBuilder.keyedBucket());
            return copiedFiltersAggregationBuilder;
        } else {
            throw new IllegalArgumentException(
                "Unsupported API Keys agg [" + aggregationBuilder.getName() + "] of type [" + aggregationBuilder.getType() + "]"
            );
        }
    }
}
