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

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.quality.PrecisionAtN.Precision;
import org.elasticsearch.action.quality.PrecisionAtN.Rating;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class TransportPrecisionAtAction extends TransportAction<PrecisionAtRequest, PrecisionAtResponse> {

    private final TransportSearchAction transportSearchAction;

    @Inject
    public TransportPrecisionAtAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters, 
            TransportSearchAction transportSearchAction) {
        super(settings, PrecisionAtAction.NAME, threadPool, actionFilters);
        this.transportSearchAction = transportSearchAction;
    }

    @Override
    protected void doExecute(PrecisionAtRequest request, ActionListener<PrecisionAtResponse> listener) {
        PrecisionAtResponse response = new PrecisionAtResponse();
        PrecisionTask qualityTask = request.getTask();
        PrecisionAtN pan = new PrecisionAtN(10);

        for (Specification spec : qualityTask.getSpecifications()) {
            double precisionAtN = 0;
            Collection<String> unknownDocs = new HashSet<String>();

            SearchRequest templated = spec.getTemplatedSearchRequest();
            Collection<Intent<Rating>> intents = qualityTask.getIntents();
            for (Intent<Rating> intent : intents) {
                Map<String, String> templateParams = intent.getIntentParameters();
                templated.templateParams(templateParams);
                ActionFuture<SearchResponse> searchResponse = transportSearchAction.execute(templated);
                SearchHits hits = searchResponse.actionGet().getHits();

                Precision precision = pan.evaluate(intent.getRatedDocuments(), hits.getHits());
                precisionAtN += precision.getPrecision();
                unknownDocs.addAll(precision.getUnknownDocs());
            }
            response.addPrecisionAt(spec.getSpecId(), precisionAtN / intents.size(), unknownDocs);
        }

        listener.onResponse(response);
    }
    
    
    
    
    
    
    
    
    
    
    
    

}
