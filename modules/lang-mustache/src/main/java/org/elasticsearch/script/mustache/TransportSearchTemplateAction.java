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
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;

public class TransportSearchTemplateAction extends HandledTransportAction<SearchTemplateRequest, SearchTemplateResponse> {

    private static final String TEMPLATE_LANG = MustacheScriptEngine.NAME;

    private final ScriptService scriptService;
    private final TransportSearchAction searchAction;
    private final NamedXContentRegistry xContentRegistry;
    private volatile ByteSizeValue maxSearchContentLength;

    @Inject
    public TransportSearchTemplateAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                         ActionFilters actionFilters, IndexNameExpressionResolver resolver,
                                         ScriptService scriptService,
                                         TransportSearchAction searchAction,
                                         NamedXContentRegistry xContentRegistry,
                                         ClusterSettings clusterSettings) {
        super(settings, SearchTemplateAction.NAME, threadPool, transportService, actionFilters, resolver, SearchTemplateRequest::new);
        this.scriptService = scriptService;
        this.searchAction = searchAction;
        this.xContentRegistry = xContentRegistry;
        this.maxSearchContentLength = clusterSettings.get(ActionModule.SETTING_SEARCH_MAX_CONTENT_LENGTH);
        clusterSettings.addSettingsUpdateConsumer(ActionModule.SETTING_SEARCH_MAX_CONTENT_LENGTH, value -> maxSearchContentLength = value);
    }

    @Override
    protected void doExecute(SearchTemplateRequest request, ActionListener<SearchTemplateResponse> listener) {
        final SearchTemplateResponse response = new SearchTemplateResponse();
        try {
            SearchRequest searchRequest = convert(request, response, scriptService, xContentRegistry, maxSearchContentLength);
            if (searchRequest != null) {
                searchAction.execute(searchRequest, new ActionListener<SearchResponse>() {
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
                                 NamedXContentRegistry xContentRegistry, ByteSizeValue maxSearchContentLength) throws IOException {
        Script script = new Script(searchTemplateRequest.getScriptType(),
            searchTemplateRequest.getScriptType() == ScriptType.STORED ? null : TEMPLATE_LANG, searchTemplateRequest.getScript(),
                searchTemplateRequest.getScriptParams() == null ? Collections.emptyMap() : searchTemplateRequest.getScriptParams());
        TemplateScript compiledScript = scriptService.compile(script, TemplateScript.CONTEXT).newInstance(script.getParams());
        BytesReference source = new BytesArray(compiledScript.execute());
        if (source.length() > maxSearchContentLength.getBytes()) {
            throw new IllegalArgumentException("Generated search request body has a size of [" + new ByteSizeValue(source.length())
                    + "] which is larger than the configured limit of [" + maxSearchContentLength
                    + "]. If you really need to send such large requests, you can update the ["
                    + ActionModule.SETTING_SEARCH_MAX_CONTENT_LENGTH.getKey() +"] cluster setting to a higher value.");
        }
        response.setSource(source);

        SearchRequest searchRequest = searchTemplateRequest.getRequest();
        if (searchTemplateRequest.isSimulate()) {
            return null;
        }

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(xContentRegistry, source)) {
            SearchSourceBuilder builder = SearchSourceBuilder.searchSource();
            builder.parseXContent(parser);
            builder.explain(searchTemplateRequest.isExplain());
            builder.profile(searchTemplateRequest.isProfile());
            searchRequest.source(builder);
        }
        return searchRequest;
    }
}
