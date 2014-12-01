/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.triggers;

import org.elasticsearch.action.search.SearchRequest;

import java.util.Map;

/**
 */
public class TriggerResult {

    private final boolean triggered;
    private boolean throttled;
    private final SearchRequest request;
    private final Map<String, Object> response;
    private final AlertTrigger trigger;

    public TriggerResult(boolean triggered, SearchRequest request, Map<String, Object> response, AlertTrigger trigger) {
        this.triggered = triggered;
        this.request = request;
        this.response = response;
        this.trigger = trigger;
    }

    public boolean isTriggered() {
        return triggered;
    }

    public boolean isThrottled() {
        return throttled;
    }

    public void setThrottled(boolean throttled) {
        this.throttled = throttled;
    }

    public SearchRequest getRequest() {
        return request;
    }

    public Map<String, Object> getResponse() {
        return response;
    }

    public AlertTrigger getTrigger() {
        return trigger;
    }

}
