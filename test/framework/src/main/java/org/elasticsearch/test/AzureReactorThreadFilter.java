/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
