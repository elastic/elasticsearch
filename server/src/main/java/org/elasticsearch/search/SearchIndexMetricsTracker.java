/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.index.Index;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tracks search metrics per index.
 */
public class SearchIndexMetricsTracker implements ClusterStateListener {

    private final ConcurrentHashMap<String, LongAdder> indexExecutionTimes = new ConcurrentHashMap<>();

    void addExecutionTime(String indexName, long executionTime) {
        indexExecutionTimes.putIfAbsent(indexName, new LongAdder());
        LongAdder indexExecutionTime = indexExecutionTimes.get(indexName);
        if (indexExecutionTime != null) {
            indexExecutionTime.add(executionTime);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        for (Index index : event.indicesDeleted()) {
            indexExecutionTimes.remove(index.getName());
        }
    }
}
