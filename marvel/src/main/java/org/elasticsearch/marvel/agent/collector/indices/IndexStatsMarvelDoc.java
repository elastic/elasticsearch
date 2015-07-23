/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

public class IndexStatsMarvelDoc extends MarvelDoc<IndexStatsMarvelDoc.Payload> {

    private final Payload payload;

    public IndexStatsMarvelDoc(String clusterName, String type, long timestamp, Payload payload) {
        super(clusterName, type, timestamp);
        this.payload = payload;
    }

    @Override
    public IndexStatsMarvelDoc.Payload payload() {
        return payload;
    }

    public static IndexStatsMarvelDoc createMarvelDoc(String clusterName, String type, long timestamp, IndexStats indexStats) {
        return new IndexStatsMarvelDoc(clusterName, type, timestamp, new Payload(indexStats));
    }

    public static class Payload {

        private final IndexStats indexStats;

        Payload(IndexStats indexStats) {
            this.indexStats = indexStats;
        }

        public IndexStats getIndexStats() {
            return indexStats;
        }
    }
}
