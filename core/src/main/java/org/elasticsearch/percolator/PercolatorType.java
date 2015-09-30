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

package org.elasticsearch.percolator;

import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.percolate.PercolateShardResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.HasContextAndHeaders;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.percolator.PercolatorQueriesRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;

abstract class PercolatorType<C extends Collector> {

    private final BigArrays bigArrays;
    private final ScriptService scriptService;

    protected PercolatorType(BigArrays bigArrays, ScriptService scriptService) {
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
    }

    // 0x00 is reserved for empty type.
    abstract byte id();

    abstract PercolatorService.ReduceResult reduce(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext);

    C doPercolate(Query percolateQuery, Query aliasQuery, Query percolateTypeQuery, PercolatorQueriesRegistry queriesRegistry, IndexSearcher shardSearcher, IndexSearcher percolateSearcher, int size, Collector... extraCollectors) throws IOException {
        C typeCollector = getCollector(size);
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        if (queriesRegistry.indexSettings().getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null).onOrAfter(Version.V_2_1_0)) {
            builder.add(queriesRegistry.getQueryMetadataService().createQueryMetadataQuery(percolateSearcher.getIndexReader()), FILTER);
        }
        if (percolateQuery != null){
            builder.add(percolateQuery, MUST);
        }
        if (aliasQuery != null) {
            builder.add(aliasQuery, FILTER);
        }
        builder.add(percolateTypeQuery, FILTER);
        Query query = builder.build();
        PercolatorQuery percolatorQuery = new PercolatorQuery(query, percolateSearcher, queriesRegistry.getPercolateQueries());
        List<Collector> collectors = new ArrayList<>();
        collectors.add(typeCollector);
        if (extraCollectors != null) {
            collectors.addAll(Arrays.asList(extraCollectors));
        }
        shardSearcher.search(percolatorQuery, MultiCollector.wrap(collectors));
        return typeCollector;
    }

    protected abstract C getCollector(int size);

    protected abstract PercolateShardResponse processResults(PercolateContext context, PercolatorQueriesRegistry registry, C collector) throws IOException;

    protected InternalAggregations reduceAggregations(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
        if (shardResults.get(0).aggregations() == null) {
            return null;
        }

        List<InternalAggregations> aggregationsList = new ArrayList<>(shardResults.size());
        for (PercolateShardResponse shardResult : shardResults) {
            aggregationsList.add(shardResult.aggregations());
        }
        InternalAggregations aggregations = InternalAggregations.reduce(aggregationsList, new InternalAggregation.ReduceContext(bigArrays, scriptService,
                headersContext));
        if (aggregations != null) {
            List<SiblingPipelineAggregator> pipelineAggregators = shardResults.get(0).pipelineAggregators();
            if (pipelineAggregators != null) {
                List<InternalAggregation> newAggs = StreamSupport.stream(aggregations.spliterator(), false).map((p) -> {
                    return (InternalAggregation) p;
                }).collect(Collectors.toList());
                for (SiblingPipelineAggregator pipelineAggregator : pipelineAggregators) {
                    InternalAggregation newAgg = pipelineAggregator.doReduce(new InternalAggregations(newAggs), new InternalAggregation.ReduceContext(
                            bigArrays, scriptService, headersContext));
                    newAggs.add(newAgg);
                }
                aggregations = new InternalAggregations(newAggs);
            }
        }
        return aggregations;
    }

}
