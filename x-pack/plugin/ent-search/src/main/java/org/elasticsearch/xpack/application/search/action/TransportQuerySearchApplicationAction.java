/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.search.SearchApplication;

import java.io.IOException;

public class TransportQuerySearchApplicationAction extends SearchApplicationTransportAction<
    QuerySearchApplicationAction.Request,
    SearchResponse> {

    private static final Logger logger = LogManager.getLogger(TransportQuerySearchApplicationAction.class);

    private final Client client;
    private final ScriptService scriptService;

    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportQuerySearchApplicationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        NamedWriteableRegistry namedWriteableRegistry,
        NamedXContentRegistry xContentRegistry,
        BigArrays bigArrays,
        ScriptService scriptService,
        XPackLicenseState licenseState
    ) {
        super(
            QuerySearchApplicationAction.NAME,
            transportService,
            actionFilters,
            QuerySearchApplicationAction.Request::new,
            client,
            clusterService,
            namedWriteableRegistry,
            bigArrays,
            licenseState
        );
        this.client = client;
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected void doExecute(QuerySearchApplicationAction.Request request, ActionListener<SearchResponse> listener) {
        systemIndexService.getSearchApplication(request.getName(), new ActionListener<>() {
            @Override
            public void onResponse(SearchApplication searchApplication) {
                final Script script = searchApplication.searchApplicationTemplate().script();
                final SearchSourceBuilder source;
                try {
                    source = applyTemplate(scriptService, script, xContentRegistry);
                } catch (IOException exc) {
                    listener.onFailure(new RuntimeException(exc));
                    return;
                }
                SearchRequest request = new SearchRequest(searchApplication.indices()).source(source);
                SearchTemplateRequest templateRequest = new SearchTemplateRequest(request);
                // logger.info(templateRequest);

                client.execute(SearchAction.INSTANCE, request, new ActionListener<>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        listener.onResponse(searchResponse);
                    }

                    @Override
                    public void onFailure(Exception exc) {
                        listener.onFailure(exc);
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private static SearchSourceBuilder applyTemplate(ScriptService scriptService, Script script, NamedXContentRegistry xContentRegistry)
        throws IOException {
        TemplateScript compiledTemplate = scriptService.compile(script, TemplateScript.CONTEXT).newInstance(script.getParams());
        String request_str = compiledTemplate.execute();
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry)
            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, request_str)) {
            SearchSourceBuilder builder = SearchSourceBuilder.searchSource();
            builder.parseXContent(parser, false);
            return builder;
        }
    }
}
