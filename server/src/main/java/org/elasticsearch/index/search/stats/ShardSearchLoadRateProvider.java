/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import java.util.function.Supplier;

/**
 * A factory interface for creating instances of {@link ShardSearchLoadRateService} using configurable settings
 * and a time source.
 * <p>
 * This abstraction allows for flexible instantiation of load rate tracking services, potentially with
 * different strategies based on provided settings or runtime context.
 */
public interface ShardSearchLoadRateProvider {

    /**
     * The default no-op implementation of the provider, which always returns {@link ShardSearchLoadRateService#NOOP}.
     * <p>
     * Useful as a fallback or when search load rate tracking is disabled or not required.
     */
    ShardSearchLoadRateProvider DEFAULT = (settings, timeProvider) -> ShardSearchLoadRateService.NOOP;

    /**
     * Creates a new instance of {@link ShardSearchLoadRateService} using the given settings and time provider.
     *
     * @param settings     the search statistics configuration settings that may influence the service behavior
     * @param timeProvider a supplier of the current time, typically in milliseconds or nanoseconds
     * @return a {@code ShardSearchLoadRateService} instance configured with the provided context
     */
    ShardSearchLoadRateService create(SearchStatsSettings settings, Supplier<Long> timeProvider);
}
