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

package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * An aggregator that is not collected, this can typically be used when running an aggregation over a field that doesn't have
 * a mapping.
 */
public abstract class NonCollectingAggregator extends AggregatorBase {

    protected NonCollectingAggregator(String name, SearchContext context, Aggregator parent, AggregatorFactories subFactories,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, subFactories, context, parent, pipelineAggregators, metaData);
    }

    protected NonCollectingAggregator(String name, SearchContext context, Aggregator parent,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        this(name, context, parent, AggregatorFactories.EMPTY, pipelineAggregators, metaData);
    }

    @Override
    public final LeafBucketCollector getLeafCollector(LeafReaderContext reader, LeafBucketCollector sub) {
        // the framework will automatically eliminate it
        return LeafBucketCollector.NO_OP_COLLECTOR;
    }

    @Override
    public final InternalAggregation buildAggregation(long owningBucketOrdinal) {
        return buildEmptyAggregation();
    }
}
