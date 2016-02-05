/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

public class IndicesStatsMarvelDoc extends MarvelDoc {

    private IndicesStatsResponse indicesStats;

    public IndicesStatsResponse getIndicesStats() {
        return indicesStats;
    }

    public void setIndicesStats(IndicesStatsResponse indicesStats) {
        this.indicesStats = indicesStats;
    }

}
