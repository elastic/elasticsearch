/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Excludes threads started by the Azure SDK's reactor-netty HTTP client.
 * The BlobServiceClient uses reactor-netty internally, which creates event loop
 * and scheduler threads that persist after fixture loading. These threads are
 * daemon threads and do not prevent JVM shutdown, but the thread leak detector
 * reports them as leaks.
 */
public class AzureReactorThreadFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        String name = t.getName();
        return name.startsWith("reactor-http-nio-") || name.startsWith("boundedElastic-");
    }
}
