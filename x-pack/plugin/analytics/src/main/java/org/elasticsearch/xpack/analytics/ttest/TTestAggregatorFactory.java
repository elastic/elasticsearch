/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

class TTestAggregatorFactory extends MultiValuesSourceAggregatorFactory {

    private final TTestType testType;
    private final int tails;

    TTestAggregatorFactory(String name, Map<String, ValuesSourceConfig> configs, TTestType testType, int tails,
                           DocValueFormat format, QueryShardContext queryShardContext, AggregatorFactory parent,
                           AggregatorFactories.Builder subFactoriesBuilder,
                           Map<String, Object> metadata) throws IOException {
        super(name, configs, format, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.testType = testType;
        this.tails = tails;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                        Aggregator parent,
                                        Map<String, Object> metadata) throws IOException {
        switch (testType) {
            case PAIRED:
                return new PairedTTestAggregator(name, null, tails, format, searchContext, parent, metadata);
            case HOMOSCEDASTIC:
                return new UnpairedTTestAggregator(name, null, tails, true, format, searchContext, parent, metadata);
            case HETEROSCEDASTIC:
                return new UnpairedTTestAggregator(name, null, tails, false, format, searchContext, parent, metadata);
            default:
                throw new IllegalArgumentException("Unsupported t-test type " + testType);
        }
    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext,
                                          Map<String, ValuesSourceConfig> configs,
                                          DocValueFormat format,
                                          Aggregator parent,
                                          boolean collectsFromSingleBucket,
                                          Map<String, Object> metadata) throws IOException {
        MultiValuesSource.NumericMultiValuesSource numericMultiVS
            = new MultiValuesSource.NumericMultiValuesSource(configs, queryShardContext);
        if (numericMultiVS.areValuesSourcesEmpty()) {
            return createUnmapped(searchContext, parent, metadata);
        }
        switch (testType) {
            case PAIRED:
                return new PairedTTestAggregator(name, numericMultiVS, tails, format, searchContext, parent, metadata);
            case HOMOSCEDASTIC:
                return new UnpairedTTestAggregator(name, numericMultiVS, tails, true, format, searchContext, parent, metadata);
            case HETEROSCEDASTIC:
                return new UnpairedTTestAggregator(name, numericMultiVS, tails, false, format, searchContext, parent, metadata);
            default:
                throw new IllegalArgumentException("Unsupported t-test type " + testType);
        }
    }
}
