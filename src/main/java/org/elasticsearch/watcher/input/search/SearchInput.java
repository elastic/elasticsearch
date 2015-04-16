/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.input.InputException;
import org.elasticsearch.watcher.support.SearchRequestEquivalence;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.WatcherUtils;
import org.elasticsearch.watcher.support.XContentFilterKeysUtils;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.execution.WatchExecutionContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentParser.*;

/**
 * An input that executes search and returns the search response as the initial payload
 */
public class SearchInput extends Input<SearchInput.Result> {

    public static final String TYPE = "search";

    public static final SearchType DEFAULT_SEARCH_TYPE = SearchType.QUERY_THEN_FETCH;

    private final Set<String> extractKeys;
    private final SearchRequest searchRequest;

    private final ScriptServiceProxy scriptService;
    private final ClientProxy client;

    public SearchInput(ESLogger logger, ScriptServiceProxy scriptService, ClientProxy client, SearchRequest searchRequest, Set<String> extractKeys) {
        super(logger);
        this.extractKeys = extractKeys;
        this.searchRequest = searchRequest;
        this.scriptService = scriptService;
        this.client = client;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Result execute(WatchExecutionContext ctx) throws IOException {

        SearchRequest request = createSearchRequestWithTimes(this.searchRequest, ctx, scriptService);
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] running query for [{}] [{}]", ctx.id(), ctx.watch().name(), XContentHelper.convertToJson(request.source(), false, true));
        }

        // actionGet deals properly with InterruptedException
        SearchResponse response = client.search(request);
        if (logger.isDebugEnabled()) {
            logger.debug("[{}] found [{}] hits", ctx.id(), ctx.watch().name(), response.getHits().getTotalHits());
            for (SearchHit hit : response.getHits()) {
                logger.debug("[{}] hit [{}]", ctx.id(), XContentHelper.toString(hit));
            }

        }

        final Payload payload;
        if (extractKeys != null) {
            XContentBuilder builder = jsonBuilder().startObject().value(response).endObject();
            XContentParser parser = XContentHelper.createParser(builder.bytes());
            Map<String, Object> filteredKeys = XContentFilterKeysUtils.filterMapOrdered(extractKeys, parser);
            payload = new Payload.Simple(filteredKeys);
        } else {
            payload = new Payload.XContent(response);
        }
        return new Result(TYPE, payload, request);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Parser.REQUEST_FIELD.getPreferredName());
        builder = WatcherUtils.writeSearchRequest(searchRequest, builder, params);
        if (extractKeys != null) {
            builder.startArray(Parser.EXTRACT_FIELD.getPreferredName());
            for (String extractKey : extractKeys) {
                builder.value(extractKey);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SearchInput that = (SearchInput) o;

        if (!SearchRequestEquivalence.INSTANCE.equivalent(searchRequest, that.searchRequest)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return SearchRequestEquivalence.INSTANCE.hash(searchRequest);
    }

    /**
     * Creates a new search request applying the scheduledFireTime and fireTime to the original request
     */
    public static SearchRequest createSearchRequestWithTimes(SearchRequest requestPrototype, WatchExecutionContext ctx, ScriptServiceProxy scriptService) throws IOException {
        SearchRequest request = new SearchRequest(requestPrototype)
                .indicesOptions(requestPrototype.indicesOptions())
                .searchType(requestPrototype.searchType())
                .indices(requestPrototype.indices());
        if (Strings.hasLength(requestPrototype.source())) {
            Map<String, Object> templateParams = Variables.createCtxModel(ctx, null);
            String requestSource = XContentHelper.convertToJson(requestPrototype.source(), false);
            ExecutableScript script = scriptService.executable("mustache", requestSource, ScriptService.ScriptType.INLINE, templateParams);
            request.source((BytesReference) script.unwrap(script.run()), false);
        } else if (requestPrototype.templateName() != null) {
            Map<String, Object> templateParams = Variables.createCtxModel(ctx, null);
            templateParams.putAll(requestPrototype.templateParams());
            request.templateParams(templateParams);
            request.templateName(requestPrototype.templateName());
            request.templateType(requestPrototype.templateType());
        }
        // falling back to an empty body
        return request;
    }

    public static class Result extends Input.Result {

        private final SearchRequest request;

        public Result(String type, Payload payload, SearchRequest request) {
            super(type, payload);
            this.request = request;
        }

        public SearchRequest request() {
            return request;
        }

        @Override
        protected XContentBuilder toXContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.field(Parser.REQUEST_FIELD.getPreferredName());
            return WatcherUtils.writeSearchRequest(request, builder, params);
        }
    }

    public static class Parser extends AbstractComponent implements Input.Parser<Result,SearchInput> {

        public static ParseField REQUEST_FIELD = new ParseField("request");
        public static ParseField EXTRACT_FIELD = new ParseField("extract");

        private final ScriptServiceProxy scriptService;
        private final ClientProxy client;

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
        public SearchInput parse(XContentParser parser) throws IOException {
            Set<String> extract = null;
            SearchRequest request = null;

            String currentFieldName = null;
            for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
                switch (token) {
                    case FIELD_NAME:
                        currentFieldName = parser.currentName();
                        break;
                    case START_OBJECT:
                        if (REQUEST_FIELD.getPreferredName().equals(currentFieldName)) {
                            request = WatcherUtils.readSearchRequest(parser, DEFAULT_SEARCH_TYPE);
                        } else {
                            throw new InputException("could not parse [search] input. unexpected field [" + currentFieldName + "]");
                        }
                        break;
                    case START_ARRAY:
                        if (EXTRACT_FIELD.getPreferredName().equals(currentFieldName)) {
                            extract = new HashSet<>();
                            for (Token arrayToken = parser.nextToken(); arrayToken != Token.END_ARRAY; arrayToken = parser.nextToken()) {
                                if (arrayToken == Token.VALUE_STRING) {
                                    extract.add(parser.text());
                                }
                            }
                        } else {
                            throw new InputException("could not parse [search] input. unexpected field [" + currentFieldName + "]");
                        }
                        break;
                    default:
                        throw new InputException("could not parse [search] input. unexpected token [" + token + "]");

                }
            }

            if (request == null) {
                throw new InputException("could not parse [search] input. search request is missing or null.");
            }
            return new SearchInput(logger, scriptService, client, request, extract);
        }

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            Payload payload = null;
            SearchRequest request = null;

            String currentFieldName = null;
            for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
                if (token == Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == Token.START_OBJECT && currentFieldName != null) {
                    if (Input.Result.PAYLOAD_FIELD.match(currentFieldName)) {
                        payload = new Payload.XContent(parser);
                    } else if (REQUEST_FIELD.match(currentFieldName)) {
                        request = WatcherUtils.readSearchRequest(parser, DEFAULT_SEARCH_TYPE);
                    } else {
                        throw new InputException("unable to parse [" + TYPE + "] input result. unexpected field [" + currentFieldName + "]");
                    }
                }
            }

            if (payload == null) {
                throw new InputException("unable to parse [" + TYPE + "] input result ["
                        + Input.Result.PAYLOAD_FIELD.getPreferredName() + "] is required");
            }

            if (request == null) {
                throw new InputException("unable to parse [" + TYPE + "] input result, ["
                        + REQUEST_FIELD.getPreferredName() + "] is required");
            }

            return new Result(TYPE, payload, request);
        }
    }

    public static class SourceBuilder implements Input.SourceBuilder {

        private final SearchRequest request;
        private Set<String> extractKeys;

        public SourceBuilder(SearchRequest request) {
            this.request = request;
        }

        public SourceBuilder addExtractKey(String key) {
            if (extractKeys == null) {
                extractKeys = new HashSet<>();
            }
            extractKeys.add(key);
            return this;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (extractKeys != null) {
                builder.startArray(Parser.EXTRACT_FIELD.getPreferredName());
                for (String extractKey : extractKeys) {
                    builder.value(extractKey);
                }
                builder.endArray();
            }
            builder.field(Parser.REQUEST_FIELD.getPreferredName());
            builder = WatcherUtils.writeSearchRequest(request, builder, params);
            return builder.endObject();
        }
    }
}
