/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.support.Variables;
import org.elasticsearch.xpack.watcher.watch.Payload;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * {@link WatcherSearchTemplateService} renders {@link WatcherSearchTemplateRequest} before their execution.
 */
public class WatcherSearchTemplateService extends AbstractComponent {

    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;

    public WatcherSearchTemplateService(Settings settings, ScriptService scriptService, NamedXContentRegistry xContentRegistry) {
        super(settings);
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
    }

    public BytesReference renderTemplate(Script source,
                                         WatchExecutionContext ctx,
                                         Payload payload) throws IOException {
        // Due the inconsistency with templates in ES 1.x, we maintain our own template format.
        // This template format we use now, will become the template structure in ES 2.0
        Map<String, Object> watcherContextParams = Variables.createCtxModel(ctx, payload);
        // Here we convert watcher template into a ES core templates. Due to the different format we use, we
        // convert to the template format used in ES core
        if (source.getParams() != null) {
            watcherContextParams.putAll(source.getParams());
        }
        // Templates are always of lang mustache:
        Script template = new Script(source.getType(), "mustache", source.getIdOrCode(), source.getOptions(), watcherContextParams
        );
        CompiledScript compiledScript = scriptService.compile(template, Watcher.SCRIPT_CONTEXT, Collections.emptyMap());
        return (BytesReference) scriptService.executable(compiledScript, template.getParams()).run();
    }

    public SearchRequest toSearchRequest(WatcherSearchTemplateRequest request) throws IOException {
        SearchRequest searchRequest = new SearchRequest(request.getIndices());
        searchRequest.types(request.getTypes());
        searchRequest.searchType(request.getSearchType());
        searchRequest.indicesOptions(request.getIndicesOptions());
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        BytesReference source = request.getSearchSource();
        if (source != null && source.length() > 0) {
            try (XContentParser parser = XContentFactory.xContent(source).createParser(xContentRegistry, source)) {
                sourceBuilder.parseXContent(new QueryParseContext(parser));
                searchRequest.source(sourceBuilder);
            }
        }
        return searchRequest;
    }
}
