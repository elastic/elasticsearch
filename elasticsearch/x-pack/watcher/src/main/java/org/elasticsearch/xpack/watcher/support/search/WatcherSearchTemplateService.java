/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.support.Variables;
import org.elasticsearch.xpack.watcher.support.WatcherScript;
import org.elasticsearch.xpack.watcher.watch.Payload;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * {@link WatcherSearchTemplateService} renders {@link WatcherSearchTemplateRequest} before their execution.
 */
public class WatcherSearchTemplateService extends AbstractComponent {

    private final ScriptService scriptService;
    private final ParseFieldMatcher parseFieldMatcher;
    private final SearchRequestParsers searchRequestParsers;

    @Inject
    public WatcherSearchTemplateService(Settings settings, ScriptService scriptService, SearchRequestParsers searchRequestParsers) {
        super(settings);
        this.scriptService = scriptService;
        this.searchRequestParsers = searchRequestParsers;
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
    }

    public BytesReference renderTemplate(WatcherScript templatePrototype,
                                         WatchExecutionContext ctx,
                                         Payload payload) throws IOException {
        // Due the inconsistency with templates in ES 1.x, we maintain our own template format.
        // This template format we use now, will become the template structure in ES 2.0
        Map<String, Object> watcherContextParams = Variables.createCtxModel(ctx, payload);
        // Here we convert watcher template into a ES core templates. Due to the different format we use, we
        // convert to the template format used in ES core
        if (templatePrototype.params() != null) {
            watcherContextParams.putAll(templatePrototype.params());
        }
        WatcherScript.Builder builder;
        if (templatePrototype.type() == ScriptService.ScriptType.INLINE) {
            builder = WatcherScript.inline(templatePrototype.script());
        } else if (templatePrototype.type() == ScriptService.ScriptType.FILE) {
            builder = WatcherScript.file(templatePrototype.script());
        } else if (templatePrototype.type() == ScriptService.ScriptType.STORED) {
            builder = WatcherScript.indexed(templatePrototype.script());
        } else {
            builder = WatcherScript.defaultType(templatePrototype.script());
        }
        WatcherScript template = builder.lang(templatePrototype.lang()).params(watcherContextParams).build();
        CompiledScript compiledScript = scriptService.compile(template.toScript(), WatcherScript.CTX, Collections.emptyMap());
        return (BytesReference) scriptService.executable(compiledScript, template.params()).run();
    }

    public SearchRequest toSearchRequest(WatcherSearchTemplateRequest request) throws IOException {
        SearchRequest searchRequest = new SearchRequest(request.getIndices());
        searchRequest.types(request.getTypes());
        searchRequest.searchType(request.getSearchType());
        searchRequest.indicesOptions(request.getIndicesOptions());
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        BytesReference source = request.getSearchSource();
        if (source != null && source.length() > 0) {
            try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
                sourceBuilder.parseXContent(new QueryParseContext(searchRequestParsers.queryParsers, parser, parseFieldMatcher),
                        searchRequestParsers.aggParsers, searchRequestParsers.suggesters);
                searchRequest.source(sourceBuilder);
            }
        }
        return searchRequest;
    }
}
