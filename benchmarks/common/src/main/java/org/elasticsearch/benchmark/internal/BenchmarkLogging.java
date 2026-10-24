/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.internal;

import org.elasticsearch.logging.internal.spi.LoggerFactory;

/**
 * Entry point for the benchmark logging bootstrap.
 *
 * <p>The {@code org.elasticsearch.logging} abstract API used by production code
 * requires a {@link LoggerFactory} provider to be installed before any call to
 * {@code LogManager.getLogger(...)}. In the server that's done by
 * {@code LogConfigurator.configureESLogging()}. Benchmarks need the same effect
 * but must not drag {@code :server} onto the {@code benchmarks/common} classpath
 * (see the module's build.gradle for the rationale), so this class installs a
 * log4j-backed factory local to this jar.
 *
 * <p>The companion {@link BenchmarkConfigurationFactory} — auto-discovered by
 * log4j via the plugin cache — sanitises any {@code %node_name} / {@code %cluster_name}
 * tokens out of {@code .properties} log4j configs on the classpath, so this
 * bootstrap does not need to seed the {@code NodeNamePatternConverter} /
 * {@code ClusterNamePatternConverter} SetOnce state that server's bootstrap has to.
 */
public final class BenchmarkLogging {

    private BenchmarkLogging() {}

    /**
     * Install the benchmark logger factory. Idempotent — repeated calls simply
     * replace the current {@link LoggerFactory} instance with an equivalent one.
     */
    public static void configure() {
        LoggerFactory.setInstance(new BenchmarkLoggerFactory());
    }
}
