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

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;

public class TransportSearchTemplateAction extends HandledTransportAction<SearchTemplateRequest, SearchTemplateResponse> {

    private static final String TEMPLATE_LANG = MustacheScriptEngine.NAME;

    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;
    private final NodeClient client;

    @Inject
    public TransportSearchTemplateAction(TransportService transportService, ActionFilters actionFilters,
                                         ScriptService scriptService, NamedXContentRegistry xContentRegistry, NodeClient client) {
        super(SearchTemplateAction.NAME, transportService, actionFilters, SearchTemplateRequest::new);
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, SearchTemplateRequest request, ActionListener<SearchTemplateResponse> listener) {
        final SearchTemplateResponse response = new SearchTemplateResponse();
        try {
            SearchRequest searchRequest = convert(request, response, scriptService, xContentRegistry);
            if (searchRequest != null) {
                client.search(searchRequest, new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        try {
                            response.setResponse(searchResponse);
                            listener.onResponse(response);
                        } catch (Exception t) {
                            listener.onFailure(t);
                        }
                    }

                    @Override
                    public void onFailure(Exception t) {
                        listener.onFailure(t);
                    }
                });
            } else {
                listener.onResponse(response);
            }
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    static SearchRequest convert(SearchTemplateRequest searchTemplateRequest, SearchTemplateResponse response, ScriptService scriptService,
                                 NamedXContentRegistry xContentRegistry) throws IOException {
        Script script = new Script(searchTemplateRequest.getScriptType(),
            searchTemplateRequest.getScriptType() == ScriptType.STORED ? null : TEMPLATE_LANG, searchTemplateRequest.getScript(),
                searchTemplateRequest.getScriptParams() == null ? Collections.emptyMap() : searchTemplateRequest.getScriptParams());
        TemplateScript compiledScript = scriptService.compile(script, TemplateScript.CONTEXT).newInstance(script.getParams());
        String source = compiledScript.execute();
        response.setSource(new BytesArray(source));

        SearchRequest searchRequest = searchTemplateRequest.getRequest();
        if (searchTemplateRequest.isSimulate()) {
            return null;
        }

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, source)) {
            SearchSourceBuilder builder = SearchSourceBuilder.searchSource();
            builder.parseXContent(parser, false);
            builder.explain(searchTemplateRequest.isExplain());
            builder.profile(searchTemplateRequest.isProfile());
            checkRestTotalHitsAsInt(searchRequest, builder);
            searchRequest.source(builder);
        }
        return searchRequest;
    }

    private static void checkRestTotalHitsAsInt(SearchRequest searchRequest, SearchSourceBuilder searchSourceBuilder) {
        if (searchRequest.source() == null) {
            searchRequest.source(new SearchSourceBuilder());
        }
        Integer trackTotalHitsUpTo = searchRequest.source().trackTotalHitsUpTo();
        // trackTotalHitsUpTo is set to Integer.MAX_VALUE when `rest_total_hits_as_int` is true
        if (trackTotalHitsUpTo != null) {
            if (searchSourceBuilder.trackTotalHitsUpTo() == null) {
                // trackTotalHitsUpTo should be set here, ensure that we can get an accurate total hits count
                searchSourceBuilder.trackTotalHitsUpTo(trackTotalHitsUpTo);
            } else if (searchSourceBuilder.trackTotalHitsUpTo() != SearchContext.TRACK_TOTAL_HITS_ACCURATE
                && searchSourceBuilder.trackTotalHitsUpTo() != SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                throw new IllegalArgumentException("[" + RestSearchAction.TOTAL_HITS_AS_INT_PARAM + "] cannot be used " +
                    "if the tracking of total hits is not accurate, got " + searchSourceBuilder.trackTotalHitsUpTo());
            }
        }
    }
}
