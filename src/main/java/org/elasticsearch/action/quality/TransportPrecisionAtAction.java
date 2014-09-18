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
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.quality.PrecisionAtN.Precision;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Instances of this class execute a collection of search intents (read: user supplied query parameters) against a set of
 * possible search requests (read: search specifications, expressed as query/search request templates) and compares the result
 * against a set of annotated documents per search intent.
 * 
 * If any documents are returned that haven't been annotated the document id of those is returned per search intent.
 * 
 * The resulting search quality is computed in terms of precision at n and returned for each search specification for the full
 * set of search intents as averaged precision at n.
 * */
public class TransportPrecisionAtAction extends TransportAction<PrecisionAtRequest, PrecisionAtResponse> {

    private final TransportSearchAction transportSearchAction;

    @Inject
    public TransportPrecisionAtAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters, 
            TransportSearchAction transportSearchAction, TransportService transportService) {
        super(settings, PrecisionAtAction.NAME, threadPool, actionFilters);
        this.transportSearchAction = transportSearchAction;
        transportService.registerHandler(PrecisionAtAction.NAME, new TransportPrecisionAtActionHandler());
    }

    @Override
    protected void doExecute(PrecisionAtRequest request, ActionListener<PrecisionAtResponse> listener) {
        PrecisionAtResponse response = new PrecisionAtResponse();
        PrecisionTask qualityTask = request.getTask();
        PrecisionAtN pan = new PrecisionAtN(qualityTask.getConfig().getN());

        for (Specification spec : qualityTask.getSpecifications()) {
            double precisionAtN = 0;

            String specRequest = spec.getSearchRequestTemplate();
            Collection<Intent<String>> intents = qualityTask.getIntents();
            Map<Integer, Collection<String>> unknownDocs = new HashMap<Integer, Collection<String>>();
            for (Intent<String> intent : intents) {
                Map<String, String> templateParams = intent.getIntentParameters();
                Joiner.MapJoiner mapJoiner = Joiner.on("\" , \"").withKeyValueSeparator("\" : \"");
                String template = "{ \"template\" : { \"query\": " + specRequest + " }, \"params\" : {\"" +
                     mapJoiner.join(templateParams.entrySet())  
                + "\"} }";
                
                SearchRequest templated = new SearchRequest();
                templated.indices(spec.getTargetIndices());
                BytesReference bytesRef = new BytesArray(template);
                templated.templateSource(bytesRef, false);
                templated.templateType(ScriptService.ScriptType.INLINE);
                
                ActionFuture<SearchResponse> searchResponse = transportSearchAction.execute(templated);
                SearchHits hits = searchResponse.actionGet().getHits();

                Precision precision = pan.evaluate(intent.getRatedDocuments(), hits.getHits());
                precisionAtN += precision.getPrecision();
                unknownDocs.put(intent.getIntentId(), precision.getUnknownDocs());
            }
            response.addPrecisionAt(spec.getSpecId(), precisionAtN / intents.size(), unknownDocs);
        }
        listener.onResponse(response);
    }
}
