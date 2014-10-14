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

package org.elasticsearch.action.quality;

import com.google.common.base.Joiner;
import org.elasticsearch.action.bench.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Needed to be able to send requests through and execute them within e.g. the local transport.
 * */
public class TransportQualityHandler extends BaseTransportRequestHandler<QualityRequest> {

    /**Needed to execute qa requests in bench framework*/
    private Client client;
    
    @Inject
    public TransportQualityHandler(Client client) {
        this.client = client;
    }
    
    @Override
    public QualityRequest newInstance() {
        return new QualityRequest();
    }

    @Override
    public void messageReceived(QualityRequest request, TransportChannel channel) throws Exception {
        BenchmarkRequest benchRequest = new BenchmarkRequest();
        QualityTask task = request.getTask();
        RankedListQualityMetric metric = task.getQualityContext().getMetric();

        for (Specification spec : task.getSpecifications()) {
            // For each qa specification add one competitor
            String[] indices = new String[spec.getTargetIndices().size()];
            spec.getTargetIndices().toArray(indices);
            BenchmarkCompetitorBuilder competitorBuilder = (new BenchmarkCompetitorBuilder(indices));
            
            
            // For each competitor add all search requests as derived from the search intents
            String specRequest = spec.getSearchRequestTemplate();
            Collection<Intent> intents = task.getIntents();
            Map<SearchRequest, Evaluator> searchTask = new HashMap<SearchRequest, Evaluator>();
            for (Intent intent : intents) {
                Map<String, String> templateParams = intent.getIntentParameters();
                Joiner.MapJoiner mapJoiner = Joiner.on("\" , \"").withKeyValueSeparator("\" : \"");
                String template = "{ \"template\" : { \"query\": " + specRequest + " }, \"params\" : {\"" +
                     mapJoiner.join(templateParams.entrySet())  
                + "\"} }";
                
                SearchRequest templated = new SearchRequest();
                templated.indices(Arrays.copyOf(spec.getTargetIndices().toArray(), spec.getTargetIndices().size(), String[].class));
                BytesReference bytesRef = new BytesArray(template);
                templated.templateSource(bytesRef, false);
                templated.templateType(ScriptService.ScriptType.INLINE);

                // For each such search request add an evaluation function (e.g. precisionAt w/ pre defined expected results
                RankedListQualityMetric evaluator = metric.initialize(intent);
                searchTask.put(templated, evaluator);
            }
            competitorBuilder.putSearchTask(searchTask);
            BenchmarkCompetitor competitor = competitorBuilder.build();
            competitor.name("" + spec.getSpecId());
            benchRequest.addCompetitor(competitor);
        }
        
        BenchmarkResponse response = client.bench(benchRequest).get();
        channel.sendResponse(response);
    }

    @Override
    public String executor() {
        return ThreadPool.Names.SEARCH;
    }
}
