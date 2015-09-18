/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.indices;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.marvel.agent.collector.indices.IndexStatsMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.AbstractRenderer;

import java.io.IOException;

public class IndexStatsRenderer extends AbstractRenderer<IndexStatsMarvelDoc> {

    public static final String[] FILTERS = {
            "index_stats.index",
            "index_stats.primaries.docs.count",
            "index_stats.total.docs.count",
            "index_stats.total.fielddata.memory_size_in_bytes",
            "index_stats.total.indexing.index_total",
            "index_stats.total.indexing.index_time_in_millis",
            "index_stats.total.indexing.throttle_time_in_millis",
            "index_stats.total.merges.total_size_in_bytes",
            "index_stats.total.search.query_total",
            "index_stats.total.search.query_time_in_millis",
            "index_stats.total.segments.memory_in_bytes",
            "index_stats.total.store.size_in_bytes",
            "index_stats.total.store.throttle_time_in_millis",
            "index_stats.total.refresh.total_time_in_millis",
    };

    public IndexStatsRenderer() {
        super(FILTERS, true);
    }

    @Override
    protected void doRender(IndexStatsMarvelDoc marvelDoc, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.INDEX_STATS);

        IndexStats indexStats = marvelDoc.getIndexStats();
        if (indexStats != null) {
            builder.field(Fields.INDEX, indexStats.getIndex());

            builder.startObject(Fields.TOTAL);
            if (indexStats.getTotal() != null) {
                indexStats.getTotal().toXContent(builder, params);
            }
            builder.endObject();

            builder.startObject(Fields.PRIMARIES);
            if (indexStats.getPrimaries() != null) {
                indexStats.getPrimaries().toXContent(builder, params);
            }
            builder.endObject();
        }

        builder.endObject();
    }

    static final class Fields {
        static final XContentBuilderString INDEX_STATS = new XContentBuilderString("index_stats");
        static final XContentBuilderString INDEX = new XContentBuilderString("index");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString PRIMARIES = new XContentBuilderString("primaries");
    }
}
