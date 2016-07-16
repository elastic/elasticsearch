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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.support.WatcherScript;
import org.elasticsearch.xpack.watcher.support.Variables;
import org.elasticsearch.xpack.watcher.watch.Payload;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * {@link WatcherSearchTemplateService} renders {@link WatcherSearchTemplateRequest} before their execution.
 */
public class WatcherSearchTemplateService extends AbstractComponent {

    private static final String DEFAULT_LANG = "mustache";

    private final ScriptService scriptService;
    private final ParseFieldMatcher parseFieldMatcher;
    private final IndicesQueriesRegistry queryRegistry;
    private final AggregatorParsers aggsParsers;
    private final Suggesters suggesters;

    @Inject
    public WatcherSearchTemplateService(Settings settings, ScriptService scriptService,
                                        IndicesQueriesRegistry queryRegistry, AggregatorParsers aggregatorParsers, Suggesters suggesters) {
        super(settings);
        this.scriptService = scriptService;
        this.queryRegistry = queryRegistry;
        this.aggsParsers = aggregatorParsers;
        this.suggesters = suggesters;
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
    }

    public SearchRequest createSearchRequestFromPrototype(WatcherSearchTemplateRequest prototype, WatchExecutionContext ctx,
                                                          Payload payload) throws IOException {

        SearchRequest request = new SearchRequest()
                .indicesOptions(prototype.getRequest().indicesOptions())
                .searchType(prototype.getRequest().searchType())
                .indices(prototype.getRequest().indices())
                .types(prototype.getRequest().types());

        WatcherScript template = null;

        // Due the inconsistency with templates in ES 1.x, we maintain our own template format.
        // This template format we use now, will become the template structure in ES 2.0
        Map<String, Object> watcherContextParams = Variables.createCtxModel(ctx, payload);

        // Here we convert a watch search request body into an inline search template,
        // this way if any Watcher related context variables are used, they will get resolved.
        if (prototype.getRequest().source() != null) {
            try (XContentBuilder builder = jsonBuilder()) {
                prototype.getRequest().source().toXContent(builder, ToXContent.EMPTY_PARAMS);
                template = WatcherScript.inline(builder.string()).lang(DEFAULT_LANG).params(watcherContextParams).build();
            }

        } else if (prototype.getTemplate() != null) {
            // Here we convert watcher template into a ES core templates. Due to the different format we use, we
            // convert to the template format used in ES core
            WatcherScript templatePrototype = prototype.getTemplate();
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
            template = builder.lang(templatePrototype.lang()).params(watcherContextParams).build();
        }

        request.source(convert(template));
        return request;
    }

    /**
     * Converts a {@link WatcherScript} to a {@link org.elasticsearch.search.builder.SearchSourceBuilder}
     */
    private SearchSourceBuilder convert(WatcherScript template) throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        if (template == null) {
            // falling back to an empty body
            return sourceBuilder;
        }
        CompiledScript compiledScript = scriptService.compile(template.toScript(), WatcherScript.CTX, Collections.emptyMap());
        BytesReference source = (BytesReference) scriptService.executable(compiledScript, template.params()).run();
        if (source != null && source.length() > 0) {
            try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
                sourceBuilder.parseXContent(new QueryParseContext(queryRegistry, parser, parseFieldMatcher), aggsParsers, suggesters);
            }
        }
        return sourceBuilder;
    }
}
