/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

/**
 * Returns the state gathered during parsing
 */
public interface NormalisedBytesToReport {

    /**
     * The value of to be reported for RA-I metric
     */
    long raiNormalisedBytes();

    /**
     * The value of to be reported for RA-S metric
     */
    long rasNormalisedBytes();
}
