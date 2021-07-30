/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import java.util.Locale;

public enum RuleAction {
    SKIP_RESULT,
    SKIP_MODEL_UPDATE;

    /**
     * Case-insensitive from string method.
     *
     * @param value String representation
     * @return The rule action
     */
    public static RuleAction fromString(String value) {
        return RuleAction.valueOf(value.toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
