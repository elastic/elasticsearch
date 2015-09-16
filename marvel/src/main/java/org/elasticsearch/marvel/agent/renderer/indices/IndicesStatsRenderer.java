/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.indices;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.marvel.agent.collector.indices.IndicesStatsMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.AbstractRenderer;

import java.io.IOException;

public class IndicesStatsRenderer extends AbstractRenderer<IndicesStatsMarvelDoc> {

    public static final String[] FILTERS = {
            "indices_stats._all.primaries.docs.count",
            "indices_stats._all.primaries.indexing.index_time_in_millis",
            "indices_stats._all.primaries.indexing.index_total",
            "indices_stats._all.primaries.indexing.is_throttled",
            "indices_stats._all.primaries.indexing.throttle_time_in_millis",
            "indices_stats._all.primaries.search.query_time_in_millis",
            "indices_stats._all.primaries.search.query_total",
            "indices_stats._all.primaries.store.size_in_bytes",
            "indices_stats._all.total.docs.count",
            "indices_stats._all.total.indexing.index_time_in_millis",
            "indices_stats._all.total.indexing.index_total",
            "indices_stats._all.total.indexing.is_throttled",
            "indices_stats._all.total.indexing.throttle_time_in_millis",
            "indices_stats._all.total.search.query_time_in_millis",
            "indices_stats._all.total.search.query_total",
            "indices_stats._all.total.store.size_in_bytes",
    };

    public IndicesStatsRenderer() {
        super(FILTERS, true);
    }

    @Override
    protected void doRender(IndicesStatsMarvelDoc marvelDoc, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.INDICES_STATS);

        IndicesStatsResponse indicesStats = marvelDoc.getIndicesStats();
        if (indicesStats != null) {
            indicesStats.toXContent(builder, params);
        }

        builder.endObject();
    }

    static final class Fields {
        static final XContentBuilderString INDICES_STATS = new XContentBuilderString("indices_stats");
    }
}
