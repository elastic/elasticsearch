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
public interface SlowLogFields {

    /**
     * Slow log fields for indexing events
     * @return map of field name to value
     */
    Map<String, String> indexFields();

    /**
     * Slow log fields for search events
     * @return map of field name to value
     */
    Map<String, String> searchFields();

    /**
     * Slow log fields for query
     * @return map of field name to value
     */
    default Map<String, String> queryFields() {
        return Map.of();
    }
}
