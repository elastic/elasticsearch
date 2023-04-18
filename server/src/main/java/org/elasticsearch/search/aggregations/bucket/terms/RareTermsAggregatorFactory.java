/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RareTermsAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final RareTermsAggregatorSupplier aggregatorSupplier;
    private final IncludeExclude includeExclude;
    private final int maxDocCount;
    private final double precision;

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            RareTermsAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.KEYWORD, CoreValuesSourceType.IP),
            RareTermsAggregatorFactory.bytesSupplier(),
            true
        );

        builder.register(
            RareTermsAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.NUMERIC),
            RareTermsAggregatorFactory.numericSupplier(),
            true
        );
    }

    /**
     * This supplier is used for all the field types that should be aggregated as bytes/strings,
     * including those that need global ordinals
     */
    private static RareTermsAggregatorSupplier bytesSupplier() {
        return (name, factories, valuesSource, format, maxDocCount, precision, includeExclude, context, parent, cardinality, metadata) -> {

            ExecutionMode execution = ExecutionMode.MAP; // TODO global ords not implemented yet, only supports "map"

            if ((includeExclude != null) && (includeExclude.isRegexBased()) && format != DocValueFormat.RAW) {
                throw new IllegalArgumentException(
                    "Aggregation ["
                        + name
                        + "] cannot support "
                        + "regular expression style include/exclude settings as they can only be applied to string fields. "
                        + "Use an array of values for include/exclude clauses"
                );
            }

            return execution.create(
                name,
                factories,
                valuesSource,
                format,
                includeExclude,
                context,
                parent,
                metadata,
                maxDocCount,
                precision,
                cardinality
            );

        };
    }

    /**
     * This supplier is used for all fields that expect to be aggregated as a numeric value.
     * This includes floating points, and formatted types that use numerics internally for storage (date, boolean, etc)
     */
    private static RareTermsAggregatorSupplier numericSupplier() {
        return (name, factories, valuesSource, format, maxDocCount, precision, includeExclude, context, parent, cardinality, metadata) -> {

            if ((includeExclude != null) && (includeExclude.isRegexBased())) {
                throw new IllegalArgumentException(
                    "Aggregation ["
                        + name
                        + "] cannot support regular expression "
                        + "style include/exclude settings as they can only be applied to string fields. Use an array of numeric "
                        + "values for include/exclude clauses used to filter numeric fields"
                );
            }

            IncludeExclude.LongFilter longFilter = null;
            if (((ValuesSource.Numeric) valuesSource).isFloatingPoint()) {
                throw new IllegalArgumentException("RareTerms aggregation does not support floating point fields.");
            }
            if (includeExclude != null) {
                longFilter = includeExclude.convertToLongFilter(format);
            }
            return new LongRareTermsAggregator(
                name,
                factories,
                (ValuesSource.Numeric) valuesSource,
                format,
                context,
                parent,
                longFilter,
                maxDocCount,
                precision,
                cardinality,
                metadata
            );
        };
    }

    RareTermsAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        IncludeExclude includeExclude,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        int maxDocCount,
        double precision,
        RareTermsAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);

        this.aggregatorSupplier = aggregatorSupplier;
        this.includeExclude = includeExclude;
        this.maxDocCount = maxDocCount;
        this.precision = precision;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new UnmappedRareTerms(name, metadata);
        return new NonCollectingAggregator(name, context, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return aggregatorSupplier.build(
            name,
            factories,
            config.getValuesSource(),
            config.format(),
            maxDocCount,
            precision,
            includeExclude,
            context,
            parent,
            cardinality,
            metadata
        );
    }

    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(
                String name,
                AggregatorFactories factories,
                ValuesSource valuesSource,
                DocValueFormat format,
                IncludeExclude includeExclude,
                AggregationContext context,
                Aggregator parent,
                Map<String, Object> metadata,
                long maxDocCount,
                double precision,
                CardinalityUpperBound cardinality
            ) throws IOException {
                final IncludeExclude.StringFilter filter = includeExclude == null ? null : includeExclude.convertToStringFilter(format);
                return new StringRareTermsAggregator(
                    name,
                    factories,
                    (ValuesSource.Bytes) valuesSource,
                    format,
                    filter,
                    context,
                    parent,
                    metadata,
                    maxDocCount,
                    precision,
                    cardinality
                );
            }

            @Override
            boolean needsGlobalOrdinals() {
                return false;
            }

        };

        public static ExecutionMode fromString(String value, final DeprecationLogger deprecationLogger) {
            return switch (value) {
                case "map" -> MAP;
                default -> throw new IllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of [map]");
            };
        }

        private final ParseField parseField;

        ExecutionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        abstract Aggregator create(
            String name,
            AggregatorFactories factories,
            ValuesSource valuesSource,
            DocValueFormat format,
            IncludeExclude includeExclude,
            AggregationContext context,
            Aggregator parent,
            Map<String, Object> metadata,
            long maxDocCount,
            double precision,
            CardinalityUpperBound cardinality
        ) throws IOException;

        abstract boolean needsGlobalOrdinals();

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }

}
