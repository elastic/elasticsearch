/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import java.util.Locale;

public enum LongDocumentStrategy {
    CHUNK("chunk"),
    TRUNCATE("truncate");

    public final String strategyName;

    LongDocumentStrategy(String strategyName) {
        this.strategyName = strategyName;
    }

    public static LongDocumentStrategy fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }
}
