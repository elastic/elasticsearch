/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.adjacency;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AdjacencyMatrixAggregatorFactory extends AggregatorFactory {

    private final String[] keys;
    private final Weight[] weights;
    private final String separator;

    public AdjacencyMatrixAggregatorFactory(String name, List<KeyedFilter> filters, String separator,
                                            AggregationContext context, AggregatorFactory parent,
                                            AggregatorFactories.Builder subFactories, Map<String, Object> metadata) throws IOException {
        super(name, context, parent, subFactories, metadata);
        IndexSearcher contextSearcher = context.searcher();
        this.separator = separator;
        weights = new Weight[filters.size()];
        keys = new String[filters.size()];
        for (int i = 0; i < filters.size(); ++i) {
            KeyedFilter keyedFilter = filters.get(i);
            keys[i] = keyedFilter.key();
            Query filter = context.buildQuery(keyedFilter.filter());
            weights[i] = contextSearcher.createWeight(contextSearcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1f);
        }
    }

    @Override
    public Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return new AdjacencyMatrixAggregator(name, factories, separator, keys, weights, context, parent, metadata);
    }

}
