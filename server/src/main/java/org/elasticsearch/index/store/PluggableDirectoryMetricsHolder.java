/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

public interface PluggableDirectoryMetricsHolder<M extends DirectoryMetrics.PluggableMetrics<M>> {
    static <M extends DirectoryMetrics.PluggableMetrics<M>> PluggableDirectoryMetricsHolder<M> noop(M noopData) {
        return new PluggableDirectoryMetricsHolder<>() {

            @Override
            public PluggableDirectoryMetricsHolder<M> singleThreaded() {
                return this;
            }

            @Override
            public M instance() {
                return noopData;
            }
        };
    }

    M instance();

    PluggableDirectoryMetricsHolder<M> singleThreaded();
}
