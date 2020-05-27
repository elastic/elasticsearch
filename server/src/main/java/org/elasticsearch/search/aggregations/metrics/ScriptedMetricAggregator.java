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

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptedMetricAggContexts;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

class ScriptedMetricAggregator extends MetricsAggregator {

    private final ScriptedMetricAggContexts.MapScript.LeafFactory mapScript;
    private final ScriptedMetricAggContexts.CombineScript combineScript;
    private final Script reduceScript;
    private Map<String, Object> aggState;

    ScriptedMetricAggregator(String name,
                                ScriptedMetricAggContexts.MapScript.LeafFactory mapScript,
                                ScriptedMetricAggContexts.CombineScript combineScript,
                                Script reduceScript,
                                Map<String, Object> aggState,
                                SearchContext context,
                                Aggregator parent,
                                Map<String, Object> metadata) throws IOException {
        super(name, context, parent, metadata);
        this.aggState = aggState;
        this.mapScript = mapScript;
        this.combineScript = combineScript;
        this.reduceScript = reduceScript;
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE; // TODO: how can we know if the script relies on scores?
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        final ScriptedMetricAggContexts.MapScript leafMapScript = mapScript.newInstance(ctx);
        return new LeafBucketCollectorBase(sub, leafMapScript) {
            @Override
            public void setScorer(Scorable scorer) throws IOException {
                leafMapScript.setScorer(scorer);
            }

            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0 : bucket;

                leafMapScript.setDocument(doc);
                leafMapScript.execute();
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        Object aggregation;
        if (combineScript != null) {
            aggregation = combineScript.execute();
            CollectionUtils.ensureNoSelfReferences(aggregation, "Scripted metric aggs combine script");
        } else {
            aggregation = aggState;
        }
        StreamOutput.checkWriteable(aggregation);
        return new InternalScriptedMetric(name, aggregation, reduceScript, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalScriptedMetric(name, null, reduceScript, metadata());
    }

    @Override
    protected void doPostCollection() throws IOException {
        CollectionUtils.ensureNoSelfReferences(aggState, "Scripted metric aggs map script");

        super.doPostCollection();
    }
}
