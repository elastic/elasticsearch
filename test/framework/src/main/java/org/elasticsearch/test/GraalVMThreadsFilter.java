/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * The GraalVM spawns extra threads, which causes our thread leak
 * detection to fail. Filter these threads out since we can't clean them up.
 */
public class GraalVMThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("Libgraal");
    }
}
