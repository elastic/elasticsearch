/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.input.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.input.Input;
import org.elasticsearch.alerts.input.InputException;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.support.SearchRequestEquivalence;
import org.elasticsearch.alerts.support.Variables;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.alerts.support.AlertsDateUtils.formatDate;

/**
 * This class just defines an input that runs a search
 */
public class SearchInput extends Input<SearchInput.Result> {

    public static final String TYPE = "search";

    public static final SearchType DEFAULT_SEARCH_TYPE = SearchType.COUNT;

    private final SearchRequest searchRequest;

    private final ScriptServiceProxy scriptService;
    private final ClientProxy client;

    public SearchInput(ESLogger logger, ScriptServiceProxy scriptService, ClientProxy client, SearchRequest searchRequest) {
        super(logger);
        this.searchRequest = searchRequest;
        this.scriptService = scriptService;
        this.client = client;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Result execute(ExecutionContext ctx) throws IOException {

        SearchRequest request = createSearchRequestWithTimes(this.searchRequest, ctx.scheduledTime(), ctx.fireTime(), ctx.executionTime(), scriptService);
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] running query for [{}] [{}]", ctx.id(), ctx.alert().name(), XContentHelper.convertToJson(request.source(), false, true));
        }

        // actionGet deals properly with InterruptedException
        SearchResponse response = client.search(request);
        if (logger.isDebugEnabled()) {
            logger.debug("[{}] found [{}] hits", ctx.id(), ctx.alert().name(), response.getHits().getTotalHits());
            for (SearchHit hit : response.getHits()) {
                logger.debug("[{}] hit [{}]", ctx.id(), XContentHelper.toString(hit));
            }

        }

        return new Result(TYPE, new Payload.XContent(response), request);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return AlertUtils.writeSearchRequest(searchRequest, builder, params);
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
    public static SearchRequest createSearchRequestWithTimes(SearchRequest requestPrototype, DateTime scheduledFireTime, DateTime fireTime, DateTime executionTime, ScriptServiceProxy scriptService) throws IOException {
        SearchRequest request = new SearchRequest(requestPrototype)
                .indicesOptions(requestPrototype.indicesOptions())
                .searchType(requestPrototype.searchType())
                .indices(requestPrototype.indices());
        if (Strings.hasLength(requestPrototype.source())) {
            Map<String, String> templateParams = new HashMap<>();
            templateParams.put(Variables.SCHEDULED_FIRE_TIME, formatDate(scheduledFireTime));
            templateParams.put(Variables.FIRE_TIME, formatDate(fireTime));
            templateParams.put(Variables.EXECUTION_TIME, formatDate(executionTime));
            String requestSource = XContentHelper.convertToJson(requestPrototype.source(), false);
            ExecutableScript script = scriptService.executable("mustache", requestSource, ScriptService.ScriptType.INLINE, templateParams);
            request.source((BytesReference) script.unwrap(script.run()), false);
        } else if (requestPrototype.templateName() != null) {
            MapBuilder<String, String> templateParams = MapBuilder.newMapBuilder(requestPrototype.templateParams())
                    .put(Variables.SCHEDULED_FIRE_TIME, formatDate(scheduledFireTime))
                    .put(Variables.FIRE_TIME, formatDate(fireTime))
                    .put(Variables.EXECUTION_TIME, formatDate(executionTime));
            request.templateParams(templateParams.map());
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
            return AlertUtils.writeSearchRequest(request, builder, params);
        }
    }

    public static class Parser extends AbstractComponent implements Input.Parser<Result,SearchInput> {

        public static ParseField REQUEST_FIELD = new ParseField("request");

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
            SearchRequest request = AlertUtils.readSearchRequest(parser, DEFAULT_SEARCH_TYPE);
            if (request == null) {
                throw new InputException("could not parse [search] input. search request is missing or null.");
            }
            return new SearchInput(logger, scriptService, client, request);
        }

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            Payload payload = null;
            SearchRequest request = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT && currentFieldName != null) {
                    if (Input.Result.PAYLOAD_FIELD.match(currentFieldName)) {
                        payload = new Payload.XContent(parser);
                    } else if (REQUEST_FIELD.match(currentFieldName)) {
                        request = AlertUtils.readSearchRequest(parser, DEFAULT_SEARCH_TYPE);
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
