/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.SearchUsageHolder;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Predicate;

public class TransportSearchTemplateAction extends HandledTransportAction<SearchTemplateRequest, SearchTemplateResponse> {

    private static final String TEMPLATE_LANG = MustacheScriptEngine.NAME;

    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;
    private final Predicate<NodeFeature> clusterSupportsFeature;
    private final NodeClient client;
    private final SearchUsageHolder searchUsageHolder;

    @Inject
    public TransportSearchTemplateAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        NodeClient client,
        UsageService usageService,
        ClusterService clusterService,
        FeatureService featureService
    ) {
        super(
            MustachePlugin.SEARCH_TEMPLATE_ACTION.name(),
            transportService,
            actionFilters,
            SearchTemplateRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
        this.clusterSupportsFeature = f -> {
            ClusterState state = clusterService.state();
            return state.clusterRecovered() && featureService.clusterHasFeature(state, f);
        };
        this.client = client;
        this.searchUsageHolder = usageService.getSearchUsageHolder();
    }

    @Override
    protected void doExecute(Task task, SearchTemplateRequest request, ActionListener<SearchTemplateResponse> listener) {
        final SearchTemplateResponse response = new SearchTemplateResponse();
        boolean success = false;
        try {
            SearchRequest searchRequest = convert(
                request,
                response,
                scriptService,
                xContentRegistry,
                clusterSupportsFeature,
                searchUsageHolder
            );
            if (searchRequest != null) {
                client.search(searchRequest, listener.delegateResponse((l, e) -> {
                    response.decRef();
                    l.onFailure(e);
                }).delegateFailureAndWrap((l, searchResponse) -> {
                    response.setResponse(searchResponse);
                    searchResponse.incRef();
                    ActionListener.respondAndRelease(l, response);
                }));
                success = true;
            } else {
                success = true;
                ActionListener.respondAndRelease(listener, response);
            }
        } catch (IOException e) {
            listener.onFailure(e);
        } finally {
            if (success == false) {
                response.decRef();
            }
        }
    }

    static SearchRequest convert(
        SearchTemplateRequest searchTemplateRequest,
        SearchTemplateResponse response,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Predicate<NodeFeature> clusterSupportsFeature,
        SearchUsageHolder searchUsageHolder
    ) throws IOException {
        Script script = new Script(
            searchTemplateRequest.getScriptType(),
            searchTemplateRequest.getScriptType() == ScriptType.STORED ? null : TEMPLATE_LANG,
            searchTemplateRequest.getScript(),
            searchTemplateRequest.getScriptParams() == null ? Collections.emptyMap() : searchTemplateRequest.getScriptParams()
        );
        TemplateScript compiledScript = scriptService.compile(script, TemplateScript.CONTEXT).newInstance(script.getParams());
        String source = compiledScript.execute();
        response.setSource(new BytesArray(source));

        SearchRequest searchRequest = searchTemplateRequest.getRequest();

        SearchSourceBuilder builder = SearchSourceBuilder.searchSource();

        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry)
            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, source)) {
            builder.parseXContent(parser, false, searchUsageHolder, clusterSupportsFeature);
        }

        if (searchTemplateRequest.isSimulate()) {
            return null;
        }
        builder.explain(searchTemplateRequest.isExplain());
        builder.profile(searchTemplateRequest.isProfile());
        checkRestTotalHitsAsInt(searchRequest, builder);
        searchRequest.source(builder);
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
                    throw new IllegalArgumentException(
                        "["
                            + RestSearchAction.TOTAL_HITS_AS_INT_PARAM
                            + "] cannot be used "
                            + "if the tracking of total hits is not accurate, got "
                            + searchSourceBuilder.trackTotalHitsUpTo()
                    );
                }
        }
    }
}
