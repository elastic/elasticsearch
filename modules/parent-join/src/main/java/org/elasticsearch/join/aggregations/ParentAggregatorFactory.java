/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.join.aggregations;

import org.apache.lucene.search.Query;
import org.elasticsearch.search.aggregations.AggregationErrors;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.WithOrdinals;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.aggregations.support.AggregationUsageService.OTHER_SUBTYPE;

public class ParentAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final Query parentFilter;
    private final Query childFilter;

    public ParentAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        Query childFilter,
        Query parentFilter,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);

        this.childFilter = childFilter;
        this.parentFilter = parentFilter;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new NonCollectingAggregator(name, context, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return new InternalParent(name, 0, buildEmptySubAggregations(), metadata());
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator children, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        ValuesSource rawValuesSource = config.getValuesSource();
        if (rawValuesSource instanceof WithOrdinals == false) {
            throw AggregationErrors.unsupportedValuesSourceType(rawValuesSource, this.name());
        }
        WithOrdinals valuesSource = (WithOrdinals) rawValuesSource;
        long maxOrd = valuesSource.globalMaxOrd(context.searcher().getIndexReader());
        return new ChildrenToParentAggregator(
            name,
            factories,
            context,
            children,
            childFilter,
            parentFilter,
            valuesSource,
            maxOrd,
            cardinality,
            metadata
        );
    }

    @Override
    public String getStatsSubtype() {
        // Parent Aggregation is registered in non-standard way
        return OTHER_SUBTYPE;
    }
}
