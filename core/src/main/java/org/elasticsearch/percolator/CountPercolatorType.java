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

import org.apache.lucene.search.TotalHitCountCollector;
import org.elasticsearch.action.percolate.PercolateShardResponse;
import org.elasticsearch.common.HasContextAndHeaders;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.percolator.PercolatorQueriesRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.List;

class CountPercolatorType extends PercolatorType<TotalHitCountCollector> {

    CountPercolatorType(BigArrays bigArrays, ScriptService scriptService) {
        super(bigArrays, scriptService);
    }

    @Override
    byte id() {
        return 0x02;
    }

    @Override
    PercolatorService.ReduceResult reduce(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
        long finalCount = 0;
        for (PercolateShardResponse shardResponse : shardResults) {
            finalCount += shardResponse.count();
        }

        assert !shardResults.isEmpty();
        InternalAggregations reducedAggregations = reduceAggregations(shardResults, headersContext);
        return new PercolatorService.ReduceResult(finalCount, reducedAggregations);
    }

    @Override
    TotalHitCountCollector getCollector(int size) {
        return new TotalHitCountCollector();
    }

    @Override
    PercolateShardResponse processResults(PercolateContext context, PercolatorQueriesRegistry registry, TotalHitCountCollector collector) {
        return new PercolateShardResponse(collector.getTotalHits(), context);
    }

}
