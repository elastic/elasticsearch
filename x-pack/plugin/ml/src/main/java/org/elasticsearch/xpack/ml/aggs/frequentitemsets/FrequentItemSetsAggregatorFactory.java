/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.AbstractItemSetMapReducer;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.InternalItemSetMapReduceAggregation;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.Tuple.tuple;

/**
 * Factory for frequent items aggregation
 *
 * Note about future readiness:
 *
 * - for improvements that do not require fundamental changes over the wire, use the usual BWC
 *   translation layers
 * - for bigger changes, implement a new map reducer and use it iff all nodes in a cluster
 *   got upgraded, however as long as a cluster has older nodes return the old the map-reducer,
 *   todo's:
 *   - add minimum node version to the AggregationContext which is created by {@link SearchService}.
 *     SearchService has access to cluster state.
 *   - wrap the new map-reducer in a new Aggregator and return the instance that all nodes in a
 *     cluster understand
 *   - add code to return the correct result reader in getResultReader below
 */
public class FrequentItemSetsAggregatorFactory extends AggregatorFactory {

    // reader that supports different versions, put here to avoid making internals public
    public static Writeable.Reader<InternalItemSetMapReduceAggregation<?, ?, ?, ?>> getResultReader() {
        return (in -> new InternalItemSetMapReduceAggregation<>(in, (mapReducerReader) -> {
            String mapReducerName = in.readString();
            if (EclatMapReducer.NAME.equals(mapReducerName)) {
                return new EclatMapReducer(FrequentItemSetsAggregationBuilder.NAME, in);
            }
            throw new AggregationExecutionException("Unknown map reducer [" + mapReducerName + "]");
        }));
    }

    private final List<MultiValuesSourceFieldConfig> fields;
    private final double minimumSupport;
    private final int minimumSetSize;
    private final int size;
    private final QueryBuilder documentFilter;
    private final String executionHint;

    public FrequentItemSetsAggregatorFactory(
        String name,
        AggregationContext context,
        AggregatorFactory parent,
        Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        List<MultiValuesSourceFieldConfig> fields,
        double minimumSupport,
        int minimumSetSize,
        int size,
        QueryBuilder documentFilter,
        String executionHint
    ) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metadata);
        this.fields = fields;
        this.minimumSupport = minimumSupport;
        this.minimumSetSize = minimumSetSize;
        this.size = size;
        this.documentFilter = documentFilter;
        this.executionHint = executionHint;
    }

    @Override
    protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {

        List<Tuple<ValuesSourceConfig, IncludeExclude>> configsAndFilters = new ArrayList<>(fields.size());
        for (MultiValuesSourceFieldConfig field : fields) {
            configsAndFilters.add(
                tuple(
                    ValuesSourceConfig.resolve(
                        context,
                        field.getUserValueTypeHint(),
                        field.getFieldName(),
                        field.getScript(),
                        field.getMissing(),
                        field.getTimeZone(),
                        field.getFormat(),
                        CoreValuesSourceType.KEYWORD
                    ),
                    field.getIncludeExclude()
                )
            );
        }

        EclatMapReducer eclatMapReducer = new EclatMapReducer(
            FrequentItemSetsAggregationBuilder.NAME,
            minimumSupport,
            minimumSetSize,
            size,
            context.profiling()
        );
        AbstractItemSetMapReducer.OrdinalOptimization ordinalOptimization = eclatMapReducer.getDefaultOrdinalOptimization();

        if ("map".equals(executionHint)) {
            ordinalOptimization = AbstractItemSetMapReducer.OrdinalOptimization.NO_ORDINALS;
        }

        return new ItemSetMapReduceAggregator<
            HashBasedTransactionStore,
            ImmutableTransactionStore,
            HashBasedTransactionStore,
            EclatMapReducer.EclatResult>(
                name,
                FrequentItemSetsAggregationBuilder.REGISTRY_KEY,
                context,
                parent,
                metadata,
                eclatMapReducer,
                configsAndFilters,
                documentFilter,
                ordinalOptimization
            ) {
        };
    }
}
