/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import java.util.Locale;

public enum RuleAction {
    FILTER_RESULTS;

    /**
     * Case-insensitive from string method.
     *
     * @param value String representation
     * @return The rule action
     */
    public static RuleAction forString(String value) {
        return RuleAction.valueOf(value.toUpperCase(Locale.ROOT));
    }
}
