/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import java.util.List;
import java.util.Map;

public class SearchAnalyticsEvent extends AnalyticsEvent {
    private static final String ACTION = "search";

    private final String engineName;

    private final String[] items;

    public SearchAnalyticsEvent(
        String collectionName,
        String engineName,
        String eventSource,
        List<Map<String, Object>> fields,
        String[] items,
        String userUUID,
        String sessionUUID
    ) {
        super(collectionName, eventSource, fields, userUUID, sessionUUID);

        this.engineName = engineName;
        this.items = items;
    }

    @Override
    protected Map<String, Object> buildEventSpecificLabels(final Map<String, Object> event) {
        event.put("labels.engineName", engineName);
        event.put("labels.items", items);

        return event;
    }

    @Override
    protected Map<String, Object> buildEventSpecificEvent(final Map<String, Object> event) {
        return event;
    }

    @Override
    public String eventAction() {
        return ACTION;
    }
}
