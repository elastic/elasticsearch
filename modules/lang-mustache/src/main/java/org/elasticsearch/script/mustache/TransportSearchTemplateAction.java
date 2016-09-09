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
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.script.ScriptContext.Standard.SEARCH;

public class TransportSearchTemplateAction extends HandledTransportAction<SearchTemplateRequest, SearchTemplateResponse> {

    private static final String TEMPLATE_LANG = MustacheScriptEngineService.NAME;

    private final ScriptService scriptService;
    private final TransportSearchAction searchAction;
    private final SearchRequestParsers searchRequestParsers;

    @Inject
    public TransportSearchTemplateAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                         ActionFilters actionFilters, IndexNameExpressionResolver resolver,
                                         ScriptService scriptService,
                                         TransportSearchAction searchAction, SearchRequestParsers searchRequestParsers) {
        super(settings, SearchTemplateAction.NAME, threadPool, transportService, actionFilters, resolver, SearchTemplateRequest::new);
        this.scriptService = scriptService;
        this.searchAction = searchAction;
        this.searchRequestParsers = searchRequestParsers;
    }

    @Override
    protected void doExecute(SearchTemplateRequest request, ActionListener<SearchTemplateResponse> listener) {
        final SearchTemplateResponse response = new SearchTemplateResponse();
        try {
            Script script = new Script(request.getScript(), request.getScriptType(), TEMPLATE_LANG, request.getScriptParams());
            ExecutableScript executable = scriptService.executable(script, SEARCH, emptyMap());

            BytesReference source = (BytesReference) executable.run();
            response.setSource(source);

            if (request.isSimulate()) {
                listener.onResponse(response);
                return;
            }

            // Executes the search
            SearchRequest searchRequest = request.getRequest();

            try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
                SearchSourceBuilder builder = SearchSourceBuilder.searchSource();
                builder.parseXContent(new QueryParseContext(searchRequestParsers.queryParsers, parser, parseFieldMatcher),
                    searchRequestParsers.aggParsers, searchRequestParsers.suggesters);
                searchRequest.source(builder);

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
            }
        } catch (Exception t) {
            listener.onFailure(t);
        }
    }
}
