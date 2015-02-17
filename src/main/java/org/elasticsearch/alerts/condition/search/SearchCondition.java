/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.condition.search;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.support.Variables;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.alerts.support.AlertsDateUtils.formatDate;

public abstract class SearchCondition extends Condition<SearchCondition.Result> {

    public static final SearchType DEFAULT_SEARCH_TYPE = SearchType.COUNT;

    protected final ScriptServiceProxy scriptService;
    protected final ClientProxy client;
    protected final SearchRequest request;

    public SearchCondition(ESLogger logger, ScriptServiceProxy scriptService, ClientProxy client, SearchRequest request) {
        super(logger);
        this.scriptService = scriptService;
        this.client = client;
        this.request = request;
    }

    @Override
    public Result execute(ExecutionContext ctx) throws IOException {
        SearchRequest request = createSearchRequestWithTimes(this.request, ctx.scheduledTime(), ctx.fireTime(), scriptService);
        if (logger.isTraceEnabled()) {
            logger.trace("running query for [{}]", ctx.alert().name(), XContentHelper.convertToJson(request.source(), false, true));
        }

        // actionGet deals properly with InterruptedException
        SearchResponse response = client.search(request).actionGet();

        if (logger.isDebugEnabled()) {
            logger.debug("got [{}] hits", ctx.alert().name(), response.getHits().getTotalHits());
            for (SearchHit hit : response.getHits()) {
                logger.debug("hit [{}]", XContentHelper.toString(hit));
            }

        }
        return processSearchResponse(response);
    }

    /**
     * Processes the search response and returns the appropriate condition result
     */
    protected abstract Result processSearchResponse(SearchResponse response);


    /**
     * Creates a new search request applying the scheduledFireTime and fireTime to the original request
     */
    public static SearchRequest createSearchRequestWithTimes(SearchRequest requestPrototype, DateTime scheduledFireTime, DateTime fireTime, ScriptServiceProxy scriptService) throws IOException {
        SearchRequest request = new SearchRequest(requestPrototype)
                .indicesOptions(requestPrototype.indicesOptions())
                .indices(requestPrototype.indices());
        if (Strings.hasLength(requestPrototype.source())) {
            Map<String, String> templateParams = new HashMap<>();
            templateParams.put(Variables.SCHEDULED_FIRE_TIME, formatDate(scheduledFireTime));
            templateParams.put(Variables.FIRE_TIME, formatDate(fireTime));
            String requestSource = XContentHelper.convertToJson(requestPrototype.source(), false);
            ExecutableScript script = scriptService.executable("mustache", requestSource, ScriptService.ScriptType.INLINE, templateParams);
            request.source((BytesReference) script.unwrap(script.run()), false);
        } else if (requestPrototype.templateName() != null) {
            MapBuilder<String, String> templateParams = MapBuilder.newMapBuilder(requestPrototype.templateParams())
                    .put(Variables.SCHEDULED_FIRE_TIME, formatDate(scheduledFireTime))
                    .put(Variables.FIRE_TIME, formatDate(fireTime));
            request.templateParams(templateParams.map());
            request.templateName(requestPrototype.templateName());
            request.templateType(requestPrototype.templateType());
        } else {
            throw new ElasticsearchIllegalStateException("Search requests needs either source or template name");
        }
        return request;
    }

    public static class Result extends Condition.Result {

        public static final ParseField REQUEST_FIELD = new ParseField("request");

        private final SearchRequest request;

        public Result(String type, boolean met, SearchRequest request, Payload payload) {
            super(type, met, payload);
            this.request = request;
        }

        public SearchRequest request() {
            return request;
        }

        @Override
        public XContentBuilder toXContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.field(REQUEST_FIELD.getPreferredName());
            AlertUtils.writeSearchRequest(request(), builder, params);
            return builder;
        }
    }

}
