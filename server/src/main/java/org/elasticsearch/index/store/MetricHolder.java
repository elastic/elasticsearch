/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

public interface MetricHolder<M> {
    static <M> MetricHolder<M> noop(M noopData) {
        return new MetricHolder<>() {

            @Override
            public MetricHolder<M> singleThreaded() {
                return this;
            }

            @Override
            public M instance() {
                return noopData;
            }
        };
    }

    M instance();

    MetricHolder<M> singleThreaded();
}
