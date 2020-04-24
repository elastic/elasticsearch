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
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RareTermsAggregatorFactory extends ValuesSourceAggregatorFactory {
    private final IncludeExclude includeExclude;
    private final int maxDocCount;
    private final double precision;

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(RareTermsAggregationBuilder.NAME,
            List.of(CoreValuesSourceType.BYTES, CoreValuesSourceType.IP),
            RareTermsAggregatorFactory.bytesSupplier());

        builder.register(RareTermsAggregationBuilder.NAME,
            List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.NUMERIC),
            RareTermsAggregatorFactory.numericSupplier());
    }

    /**
     * This supplier is used for all the field types that should be aggregated as bytes/strings,
     * including those that need global ordinals
     */
    private static RareTermsAggregatorSupplier bytesSupplier() {
        return new RareTermsAggregatorSupplier() {
            @Override
            public Aggregator build(String name,
                                    AggregatorFactories factories,
                                    ValuesSource valuesSource,
                                    DocValueFormat format,
                                    int maxDocCount,
                                    double precision,
                                    IncludeExclude includeExclude,
                                    SearchContext context,
                                    Aggregator parent,
                                    Map<String, Object> metadata) throws IOException {

                ExecutionMode execution = ExecutionMode.MAP; //TODO global ords not implemented yet, only supports "map"

                if ((includeExclude != null) && (includeExclude.isRegexBased()) && format != DocValueFormat.RAW) {
                    throw new IllegalArgumentException("Aggregation [" + name + "] cannot support " +
                        "regular expression style include/exclude settings as they can only be applied to string fields. " +
                        "Use an array of values for include/exclude clauses");
                }

                return execution.create(name, factories, valuesSource, format,
                    includeExclude, context, parent, metadata, maxDocCount, precision);

            }
        };
    }

    /**
     * This supplier is used for all fields that expect to be aggregated as a numeric value.
     * This includes floating points, and formatted types that use numerics internally for storage (date, boolean, etc)
     */
    private static RareTermsAggregatorSupplier numericSupplier() {
        return new RareTermsAggregatorSupplier() {
            @Override
            public Aggregator build(String name,
                                    AggregatorFactories factories,
                                    ValuesSource valuesSource,
                                    DocValueFormat format,
                                    int maxDocCount,
                                    double precision,
                                    IncludeExclude includeExclude,
                                    SearchContext context,
                                    Aggregator parent,
                                    Map<String, Object> metadata) throws IOException {

                if ((includeExclude != null) && (includeExclude.isRegexBased())) {
                    throw new IllegalArgumentException("Aggregation [" + name + "] cannot support regular expression " +
                        "style include/exclude settings as they can only be applied to string fields. Use an array of numeric " +
                        "values for include/exclude clauses used to filter numeric fields");
                }

                IncludeExclude.LongFilter longFilter = null;
                if (((ValuesSource.Numeric) valuesSource).isFloatingPoint()) {
                    throw new IllegalArgumentException("RareTerms aggregation does not support floating point fields.");
                }
                if (includeExclude != null) {
                    longFilter = includeExclude.convertToLongFilter(format);
                }
                return new LongRareTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource, format,
                    context, parent, longFilter, maxDocCount, precision, metadata);
            }
        };
    }

    RareTermsAggregatorFactory(String name, ValuesSourceConfig config,
                                      IncludeExclude includeExclude,
                                      QueryShardContext queryShardContext,
                                      AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder,
                                      Map<String, Object> metadata, int maxDocCount, double precision) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.includeExclude = includeExclude;
        this.maxDocCount = maxDocCount;
        this.precision = precision;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new UnmappedRareTerms(name, metadata);
        return new NonCollectingAggregator(name, searchContext, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource,
                                            SearchContext searchContext,
                                            Aggregator parent,
                                            boolean collectsFromSingleBucket,
                                            Map<String, Object> metadata) throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, searchContext, parent);
        }

        AggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry().getAggregator(config.valueSourceType(),
            RareTermsAggregationBuilder.NAME);
        if (aggregatorSupplier instanceof RareTermsAggregatorSupplier == false) {
            throw new AggregationExecutionException("Registry miss-match - expected RareTermsAggregatorSupplier, found [" +
                aggregatorSupplier.getClass().toString() + "]");
        }

        return ((RareTermsAggregatorSupplier) aggregatorSupplier).build(name, factories, valuesSource, config.format(),
            maxDocCount, precision, includeExclude, searchContext, parent, metadata);
    }

    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource,
                              DocValueFormat format, IncludeExclude includeExclude,
                              SearchContext context, Aggregator parent,
                              Map<String, Object> metadata, long maxDocCount, double precision)
                throws IOException {
                final IncludeExclude.StringFilter filter = includeExclude == null ? null : includeExclude.convertToStringFilter(format);
                return new StringRareTermsAggregator(name, factories, (ValuesSource.Bytes) valuesSource, format, filter,
                    context, parent, metadata, maxDocCount, precision);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return false;
            }

        };

        public static ExecutionMode fromString(String value, final DeprecationLogger deprecationLogger) {
            switch (value) {
                case "map":
                    return MAP;
                default:
                    throw new IllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of [map]");
            }
        }

        private final ParseField parseField;

        ExecutionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        abstract Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource,
                                   DocValueFormat format, IncludeExclude includeExclude,
                                   SearchContext context, Aggregator parent, Map<String, Object> metadata,
                                   long maxDocCount, double precision)
            throws IOException;

        abstract boolean needsGlobalOrdinals();

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }

}
