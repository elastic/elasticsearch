/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.action.search.SearchResponse;

public class AlertResult {
    public SearchResponse searchResponse;
    public AlertTrigger trigger;
    public boolean isTriggered;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AlertResult that = (AlertResult) o;

        if (isTriggered != that.isTriggered) return false;
        if (!searchResponse.equals(that.searchResponse)) return false;
        if (!trigger.equals(that.trigger)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = searchResponse.hashCode();
        result = 31 * result + trigger.hashCode();
        result = 31 * result + (isTriggered ? 1 : 0);
        return result;
    }
}
