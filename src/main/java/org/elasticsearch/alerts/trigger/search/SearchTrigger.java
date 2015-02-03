/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.trigger.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.alerts.trigger.Trigger;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.Map;

public abstract class SearchTrigger extends Trigger<SearchTrigger.Result> {

    protected final ScriptServiceProxy scriptService;
    protected final ClientProxy client;
    protected final SearchRequest request;

    public SearchTrigger(ESLogger logger, ScriptServiceProxy scriptService, ClientProxy client, SearchRequest request) {
        super(logger);
        this.scriptService = scriptService;
        this.client = client;
        this.request = request;
    }

    @Override
    public Result execute(Alert alert, DateTime scheduledFireTime, DateTime fireTime) throws IOException {
        SearchRequest request = AlertUtils.createSearchRequestWithTimes(this.request, scheduledFireTime, fireTime, scriptService);
        if (logger.isTraceEnabled()) {
            logger.trace("For alert [{}] running query for [{}]", alert.name(), XContentHelper.convertToJson(request.source(), false, true));
        }

        // actionGet deals properly with InterruptedException
        SearchResponse response = client.search(request).actionGet();

        if (logger.isDebugEnabled()) {
            logger.debug("Ran alert [{}] and got hits : [{}]", alert.name(), response.getHits().getTotalHits());
            for (SearchHit hit : response.getHits()) {
                logger.debug("Hit: {}", XContentHelper.toString(hit));
            }

        }
        return processSearchResponse(response);
    }

    /**
     * Processes the search response and returns the appropriate trigger result
     */
    protected abstract Result processSearchResponse(SearchResponse response);

    static class Result extends Trigger.Result {

        private final SearchRequest request;

        public Result(String type, boolean triggered, SearchRequest request, Map<String, Object> data) {
            super(type, triggered, data);
            this.request = request;
        }

        public SearchRequest request() {
            return request;
        }
    }

}
