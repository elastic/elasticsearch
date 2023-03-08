/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AnalyticsEvent {
    private String collectionName;
    private String eventSource;
    private String userUUID;
    private String sessionUUID;
    private List<Map<String, Object>> fields;

    public AnalyticsEvent(
        String collectionName,
        String eventSource,
        List<Map<String, Object>> fields,
        @Nullable String userUUID,
        @Nullable String sessionUUID
    ) {
        this.collectionName = collectionName;
        this.eventSource = eventSource;
        this.fields = fields;
        this.userUUID = userUUID;
        this.sessionUUID = sessionUUID;
    }

    /**
     * Convert the event to the ECS compatible format.
     * @param event Base of the event.
     *
     * @return The converted event as a map.
     */
    public ESLogMessage toESLogMessage(Map<String, Object> event) {
        event = buildEvent(event);
        event = buildLabels(event);

        return new ESLogMessage().withFields(event);
    }

    /**
     * Returns the name of the event.
     *
     * @return The name of the event.
     */
    public abstract String eventAction();

    /**
     * Returns the name of the collection to which the event should be sent.
     *
     * @return The name of the collection to which the event should be sent.
     */
    public String getCollectionName() {
        return this.collectionName;
    }

    private Map<String, Object> buildEvent(Map<String, Object> event) {
        event.put("event.name", eventAction());
        event.put("event.source", eventSource);

        return buildEventSpecificEvent(event);
    }

    private Map<String, Object> buildLabels(Map<String, Object> event) {
        if (userUUID != null) {
            event.put("labels.user_uuid", userUUID);
        }
        if (sessionUUID != null) {
            event.put("labels.session_uuid", sessionUUID);
        }
        List<Map<String, Object>> fieldLabels = new ArrayList<>();
        {
            for (Map<String, Object> field : fields) {
                Map<String, Object> fieldLabel = new HashMap<>();
                fieldLabel.put("name", field.get("name"));
                fieldLabel.put("type", field.get("type"));
                fieldLabel.put("value", field.get("value"));

                fieldLabels.add(fieldLabel);
            }
            event.put("labels.fields", fieldLabels);
        }

        return buildEventSpecificLabels(event);
    }

    protected abstract Map<String, Object> buildEventSpecificEvent(Map<String, Object> event);

    protected abstract Map<String, Object> buildEventSpecificLabels(Map<String, Object> event);
}
