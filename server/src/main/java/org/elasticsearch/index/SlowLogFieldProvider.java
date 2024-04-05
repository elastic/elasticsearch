/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import java.util.Map;

/**
 * Interface for providing additional fields to the slow log from a plugin.
 * Intended to be loaded through SPI.
 */
public interface SlowLogFieldProvider {
    /**
     * Initialize field provider with index level settings to be able to listen for updates and set initial values
     * @param indexSettings settings for the index
     */
    void init(IndexSettings indexSettings);

    /**
     * Slow log fields for indexing events
     * @return map of field name to value
     */
    Map<String, String> indexSlowLogFields();

    /**
     * Slow log fields for search events
     * @return map of field name to value
     */
    Map<String, String> searchSlowLogFields();
}
