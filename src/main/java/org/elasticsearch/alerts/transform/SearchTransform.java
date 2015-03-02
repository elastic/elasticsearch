/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transform;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.alerts.support.AlertsDateUtils.formatDate;
import static org.elasticsearch.alerts.support.Variables.createCtxModel;

/**
 *
 */
public class SearchTransform extends Transform {

    public static final String TYPE = "search";

    public static final SearchType DEFAULT_SEARCH_TYPE = SearchType.DFS_QUERY_AND_FETCH;

    protected final ESLogger logger;
    protected final ScriptServiceProxy scriptService;
    protected final ClientProxy client;

    protected final SearchRequest request;

    public SearchTransform(ESLogger logger, ScriptServiceProxy scriptService, ClientProxy client, SearchRequest request) {
        this.logger = logger;
        this.scriptService = scriptService;
        this.client = client;
        this.request = request;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Transform.Result apply(ExecutionContext ctx, Payload payload) throws IOException {
        SearchRequest req = createRequest(request, ctx, payload);
        SearchResponse resp = client.search(req).actionGet();
        return new Transform.Result(TYPE, new Payload.ActionResponse(resp));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return AlertUtils.writeSearchRequest(request, builder, params);
    }

    public SearchRequest createRequest(SearchRequest requestPrototype, ExecutionContext ctx, Payload payload) throws IOException {
        SearchRequest request = new SearchRequest(requestPrototype)
                .indicesOptions(requestPrototype.indicesOptions())
                .indices(requestPrototype.indices());
        if (Strings.hasLength(requestPrototype.source())) {
            String requestSource = XContentHelper.convertToJson(requestPrototype.source(), false);
            ExecutableScript script = scriptService.executable("mustache", requestSource, ScriptService.ScriptType.INLINE, createCtxModel(ctx, payload));
            request.source((BytesReference) script.unwrap(script.run()), false);
        } else if (requestPrototype.templateName() != null) {
            MapBuilder<String, String> templateParams = MapBuilder.newMapBuilder(requestPrototype.templateParams())
                    .putAll(flatten(createCtxModel(ctx, payload)));
            request.templateParams(templateParams.map());
            request.templateName(requestPrototype.templateName());
            request.templateType(requestPrototype.templateType());
        } else {
            throw new TransformException("search requests needs either source or template name");
        }
        return request;
    }

    static Map<String, String> flatten(Map<String, Object> map) {
        Map<String, String> result = new HashMap<>();
        flatten("", map, result);
        return result;
    }

    private static void flatten(String key, Object value, Map<String, String> result) {
        if (value instanceof Map) {
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
                if ("".equals(key)) {
                    flatten(entry.getKey(), entry.getValue(), result);
                } else {
                    flatten(key + "." + entry.getKey(), entry.getValue(), result);
                }
            }
            return;
        }
        if (value instanceof Iterable) {
            int i = 0;
            for (Object item : (Iterable) value) {
                flatten(key + "." + i++, item, result);
            }
            return;
        }
        if (value.getClass().isArray()) {
            for (int i = 0; i < Array.getLength(value); i++) {
                flatten(key + "." + i, Array.get(value, i), result);
            }
            return;
        }
        if (value instanceof DateTime) {
            result.put(key, formatDate((DateTime) value));
            return;
        }
        if (value instanceof TimeValue) {
            result.put(key, String.valueOf(((TimeValue) value).getMillis()));
            return;
        }
        result.put(key, String.valueOf(value));
    }

    public static class Parser extends AbstractComponent implements Transform.Parser<SearchTransform> {

        protected final ScriptServiceProxy scriptService;
        protected final ClientProxy client;

        @Inject
        public Parser(Settings settings, ScriptServiceProxy scriptService, ClientProxy client) {
            super(settings);
            this.scriptService = scriptService;
            this.client = client;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public SearchTransform parse(XContentParser parser) throws IOException {
            SearchRequest request = AlertUtils.readSearchRequest(parser, DEFAULT_SEARCH_TYPE);
            return new SearchTransform(logger, scriptService, client, request);
        }
    }

    public static class SourceBuilder implements Transform.SourceBuilder {

        private final SearchRequest request;

        public SourceBuilder(SearchRequest request) {
            this.request = request;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return AlertUtils.writeSearchRequest(request, builder, params);
        }
    }

}
