/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

/**
 * Implemented by the pluggable {@link DirectoryMetrics} metric that tracks how long searches waited on blob store cache misses,
 * so that search code in this module can read it without depending on the plugin that provides it.
 */
public interface CacheMissWaitMetrics {

    /** The number of times a cache miss was waited on. */
    long getWaits();

    /** The total time spent waiting on cache misses, in nanoseconds. */
    long getWaitTime();
}
