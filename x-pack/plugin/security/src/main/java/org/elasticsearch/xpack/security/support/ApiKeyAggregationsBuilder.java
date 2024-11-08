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
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.util.function.Consumer;

import static org.elasticsearch.xpack.security.support.FieldNameTranslators.API_KEY_FIELD_NAME_TRANSLATORS;

public class ApiKeyAggregationsBuilder {

    public static AggregatorFactories.Builder process(
        @Nullable AggregatorFactories.Builder aggsBuilder,
        @Nullable Consumer<String> fieldNameVisitor
    ) {
        if (aggsBuilder == null) {
            return null;
        }
        // Most of these can be supported without much hassle, but they're not useful for the identified use cases so far
        for (PipelineAggregationBuilder pipelineAggregator : aggsBuilder.getPipelineAggregatorFactories()) {
            throw new IllegalArgumentException("Unsupported aggregation of type [" + pipelineAggregator.getType() + "]");
        }
        AggregatorFactories.Builder copiedAggsBuilder = AggregatorFactories.builder();
        for (AggregationBuilder aggregationBuilder : aggsBuilder.getAggregatorFactories()) {
            copiedAggsBuilder.addAggregator(
                translateAggsFields(aggregationBuilder, fieldNameVisitor != null ? fieldNameVisitor : ignored -> {})
            );
        }
        return copiedAggsBuilder;
    }

    private static AggregationBuilder translateAggsFields(AggregationBuilder aggsBuilder, Consumer<String> fieldNameVisitor) {
        return AggregationBuilder.deepCopy(aggsBuilder, copiedAggsBuilder -> {
            // Most of these can be supported without much hassle, but they're not useful for the identified use cases so far
            for (PipelineAggregationBuilder pipelineAggregator : copiedAggsBuilder.getPipelineAggregations()) {
                throw new IllegalArgumentException("Unsupported aggregation of type [" + pipelineAggregator.getType() + "]");
            }
            if (copiedAggsBuilder instanceof ValuesSourceAggregationBuilder<?> valuesSourceAggregationBuilder) {
                if (valuesSourceAggregationBuilder instanceof TermsAggregationBuilder == false
                    && valuesSourceAggregationBuilder instanceof RangeAggregationBuilder == false
                    && valuesSourceAggregationBuilder instanceof DateRangeAggregationBuilder == false
                    && valuesSourceAggregationBuilder instanceof MissingAggregationBuilder == false
                    && valuesSourceAggregationBuilder instanceof CardinalityAggregationBuilder == false
                    && valuesSourceAggregationBuilder instanceof ValueCountAggregationBuilder == false) {
                    throw new IllegalArgumentException(
                        "Unsupported API Keys agg [" + copiedAggsBuilder.getName() + "] of type [" + copiedAggsBuilder.getType() + "]"
                    );
                }
                // scripts are not currently supported because it's harder to restrict and rename the doc fields the script has access to
                if (valuesSourceAggregationBuilder.script() != null) {
                    throw new IllegalArgumentException("Unsupported script value source for [" + copiedAggsBuilder.getName() + "] agg");
                }
                // the user-facing field names are different from the index mapping field names of API Key docs
                String translatedFieldName = API_KEY_FIELD_NAME_TRANSLATORS.translate(valuesSourceAggregationBuilder.field());
                valuesSourceAggregationBuilder.field(translatedFieldName);
                fieldNameVisitor.accept(translatedFieldName);
                return valuesSourceAggregationBuilder;
            } else if (copiedAggsBuilder instanceof CompositeAggregationBuilder compositeAggregationBuilder) {
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
                    String translatedFieldName = API_KEY_FIELD_NAME_TRANSLATORS.translate(valueSource.field());
                    valueSource.field(translatedFieldName);
                    fieldNameVisitor.accept(translatedFieldName);
                }
                return compositeAggregationBuilder;
            } else if (copiedAggsBuilder instanceof FilterAggregationBuilder filterAggregationBuilder) {
                // filters the aggregation query to user's allowed API Keys only
                FilterAggregationBuilder newFilterAggregationBuilder = new FilterAggregationBuilder(
                    filterAggregationBuilder.getName(),
                    API_KEY_FIELD_NAME_TRANSLATORS.translateQueryBuilderFields(filterAggregationBuilder.getFilter(), fieldNameVisitor)
                );
                if (filterAggregationBuilder.getMetadata() != null) {
                    newFilterAggregationBuilder.setMetadata(filterAggregationBuilder.getMetadata());
                }
                for (AggregationBuilder subAgg : filterAggregationBuilder.getSubAggregations()) {
                    newFilterAggregationBuilder.subAggregation(subAgg);
                }
                return newFilterAggregationBuilder;
            } else if (copiedAggsBuilder instanceof FiltersAggregationBuilder filtersAggregationBuilder) {
                // filters the aggregation's bucket queries to user's allowed API Keys only
                QueryBuilder[] filterQueryBuilders = new QueryBuilder[filtersAggregationBuilder.filters().size()];
                for (int i = 0; i < filtersAggregationBuilder.filters().size(); i++) {
                    filterQueryBuilders[i] = API_KEY_FIELD_NAME_TRANSLATORS.translateQueryBuilderFields(
                        filtersAggregationBuilder.filters().get(i).filter(),
                        fieldNameVisitor
                    );
                }
                final FiltersAggregationBuilder newFiltersAggregationBuilder;
                if (filtersAggregationBuilder.isKeyed()) {
                    FiltersAggregator.KeyedFilter[] keyedFilters = new FiltersAggregator.KeyedFilter[filterQueryBuilders.length];
                    for (int i = 0; i < keyedFilters.length; i++) {
                        keyedFilters[i] = new FiltersAggregator.KeyedFilter(
                            filtersAggregationBuilder.filters().get(i).key(),
                            filterQueryBuilders[i]
                        );
                    }
                    newFiltersAggregationBuilder = new FiltersAggregationBuilder(filtersAggregationBuilder.getName(), keyedFilters);
                } else {
                    newFiltersAggregationBuilder = new FiltersAggregationBuilder(filtersAggregationBuilder.getName(), filterQueryBuilders);
                }
                assert newFiltersAggregationBuilder.isKeyed() == filtersAggregationBuilder.isKeyed();
                newFiltersAggregationBuilder.otherBucket(filtersAggregationBuilder.otherBucket())
                    .otherBucketKey(filtersAggregationBuilder.otherBucketKey())
                    .keyedBucket(filtersAggregationBuilder.keyedBucket());
                for (AggregationBuilder subAgg : filtersAggregationBuilder.getSubAggregations()) {
                    newFiltersAggregationBuilder.subAggregation(subAgg);
                }
                return newFiltersAggregationBuilder;
            } else {
                // BEWARE: global agg type must NOT be supported!
                // This is because "global" breaks out of the search execution context and will expose disallowed API Keys
                // (e.g. keys with different owners), as well as other .security docs
                throw new IllegalArgumentException(
                    "Unsupported API Keys agg [" + copiedAggsBuilder.getName() + "] of type [" + copiedAggsBuilder.getType() + "]"
                );
            }
        });
    }
}
