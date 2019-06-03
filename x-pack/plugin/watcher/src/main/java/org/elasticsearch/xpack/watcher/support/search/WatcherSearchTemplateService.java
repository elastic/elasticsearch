/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * {@link WatcherSearchTemplateService} renders {@link WatcherSearchTemplateRequest} before their execution.
 */
public class WatcherSearchTemplateService {

    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;

    public WatcherSearchTemplateService(ScriptService scriptService, NamedXContentRegistry xContentRegistry) {
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
    }

    public String renderTemplate(Script source, WatchExecutionContext ctx, Payload payload) throws IOException {
        // Due the inconsistency with templates in ES 1.x, we maintain our own template format.
        // This template format we use now, will become the template structure in ES 2.0
        Map<String, Object> watcherContextParams = Variables.createCtxParamsMap(ctx, payload);
        // Here we convert watcher template into a ES core templates. Due to the different format we use, we
        // convert to the template format used in ES core
        if (source.getParams() != null) {
            watcherContextParams.putAll(source.getParams());
        }
        // Templates are always of lang mustache:
        Script template = new Script(source.getType(), source.getType() == ScriptType.STORED ? null : "mustache",
                source.getIdOrCode(), source.getOptions(), watcherContextParams);
        TemplateScript.Factory compiledTemplate = scriptService.compile(template, Watcher.SCRIPT_TEMPLATE_CONTEXT);
        return compiledTemplate.newInstance(template.getParams()).execute();
    }

    public SearchRequest toSearchRequest(WatcherSearchTemplateRequest request) throws IOException {
        SearchRequest searchRequest = new SearchRequest(request.getIndices());
        searchRequest.searchType(request.getSearchType());
        searchRequest.indicesOptions(request.getIndicesOptions());
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        BytesReference source = request.getSearchSource();
        if (source != null && source.length() > 0) {
            try (InputStream stream = source.streamInput();
                 XContentParser parser = XContentFactory.xContent(XContentHelper.xContentType(source))
                         .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
                sourceBuilder.parseXContent(parser);
                searchRequest.source(sourceBuilder);
            }
        }
        return searchRequest;
    }
}
