/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import java.util.Locale;

public enum AnalyticsEventType {
    PAGEVIEW("pageview"),
    SEARCH("search"),
    INTERACTION("interaction");

    private final String typeName;

    AnalyticsEventType(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public String toString() {
        return typeName.toLowerCase(Locale.ROOT);
    }
}
