/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import java.util.Map;

/**
 * Fields for the slow log. These may be different each call depending on the state of the system.
 */
public abstract class SlowLogFields {

    protected final SlowLogContext context;

    public SlowLogFields(SlowLogContext context) {
        this.context = context;
    }

    /**
     * Slow log fields for query
     * @return map of field name to value
     */
    public Map<String, String> logFields() {
        return Map.of();
    }
}
