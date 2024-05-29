/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Java's {@link java.net.http.HttpClient} spawns threads, which causes our thread leak
 * detection to fail. Filter these threads out since AFAICT we can't completely clean them up.
 */
public class HttpClientThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("HttpClient");
    }
}
