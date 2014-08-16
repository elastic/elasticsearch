/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.Arrays;

public class AlertResult {
    public SearchResponse searchResponse;
    public AlertTrigger trigger;
    public String alertName;

    public AlertResult(String alertName, SearchResponse searchResponse, AlertTrigger trigger, boolean isTriggered, XContentBuilder query, String[] indices) {
        this.searchResponse = searchResponse;
        this.trigger = trigger;
        this.isTriggered = isTriggered;
        this.query = query;
        this.indices = indices;
        this.alertName = alertName;
    }

    public boolean isTriggered;
    public XContentBuilder query;
    public String[] indices;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AlertResult that = (AlertResult) o;

        if (isTriggered != that.isTriggered) return false;
        if (!Arrays.equals(indices, that.indices)) return false;
        if (query != null ? !query.equals(that.query) : that.query != null) return false;
        if (searchResponse != null ? !searchResponse.equals(that.searchResponse) : that.searchResponse != null)
            return false;
        if (trigger != null ? !trigger.equals(that.trigger) : that.trigger != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = searchResponse != null ? searchResponse.hashCode() : 0;
        result = 31 * result + (trigger != null ? trigger.hashCode() : 0);
        result = 31 * result + (isTriggered ? 1 : 0);
        result = 31 * result + (query != null ? query.hashCode() : 0);
        result = 31 * result + (indices != null ? Arrays.hashCode(indices) : 0);
        return result;
    }

}
