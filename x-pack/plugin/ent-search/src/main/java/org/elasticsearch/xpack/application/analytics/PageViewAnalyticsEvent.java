/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.Map;

public class PageViewAnalyticsEvent extends AnalyticsEvent {
    private static final String ACTION = "pageView";

    private final String url;
    private final String referrer;

    public PageViewAnalyticsEvent(
        String collectionName,
        String url,
        String referrer,
        String eventSource,
        List<Map<String, Object>> fields,
        @Nullable String userUUID,
        @Nullable String sessionUUID
    ) {
        super(collectionName, eventSource, fields, userUUID, sessionUUID);

        this.url = url;
        this.referrer = referrer;
    }

    @Override
    protected Map<String, Object> buildEventSpecificEvent(final Map<String, Object> event) {
        event.put("event.url", url);
        event.put("event.referrer", referrer);

        return event;
    }

    @Override
    protected Map<String, Object> buildEventSpecificLabels(final Map<String, Object> event) {
        return event;
    }

    @Override
    public String eventAction() {
        return ACTION;
    }
}
