/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.TotalHitCountCollector;

import java.io.IOException;
import java.util.Collection;

public class TotalHitCountCollectorManager extends org.apache.lucene.search.TotalHitCountCollectorManager {
    private volatile int totalHitCount;

    @Override
    public Integer reduce(Collection<TotalHitCountCollector> collectors) throws IOException {
        this.totalHitCount = super.reduce(collectors);
        return null;
    }

    public int getTotalHitCount() {
        return totalHitCount;
    }
}
