/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import java.util.Locale;

/**
 * Interaction types.
 */
public enum InteractionType {
    CLICK("click");

    private final String typeName;

    InteractionType(String typeName) {
        this.typeName = typeName;
    }

    public String toString() {
        return typeName.toLowerCase(Locale.ROOT);
    }
}
