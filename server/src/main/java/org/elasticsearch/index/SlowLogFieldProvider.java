/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

/**
 * Interface for providing additional fields to the slow log from a plugin.
 * Intended to be loaded through SPI.
 */
public interface SlowLogFieldProvider {
    /**
     * Create a field provider with index level settings to be able to listen for updates and set initial values
     * @param indexSettings settings for the index
     */
    SlowLogFields create(IndexSettings indexSettings);

    /**
     * Create a field provider without index level settings
     */
    SlowLogFields create();
}
