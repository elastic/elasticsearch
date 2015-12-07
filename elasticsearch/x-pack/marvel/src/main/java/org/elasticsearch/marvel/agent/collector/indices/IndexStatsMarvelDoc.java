/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

public class IndexStatsMarvelDoc extends MarvelDoc {

    private final IndexStats indexStats;

    public IndexStatsMarvelDoc(String clusterUUID, String type, long timestamp, IndexStats indexStats) {
        super(clusterUUID, type, timestamp);
        this.indexStats = indexStats;
    }

    public IndexStats getIndexStats() {
        return indexStats;
    }
}
