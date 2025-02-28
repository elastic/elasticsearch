/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.hybrid;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class HybridRetrieverBuilder extends CompoundRetrieverBuilder<HybridRetrieverBuilder> {
    public static final String NAME = "hybrid";
    public static final ParseField FIELDS_FIELD = new ParseField("fields");

    private final List<String> fields;

    public HybridRetrieverBuilder(List<String> fields, int rankWindowSize) {
        super(List.of(), rankWindowSize);
        this.fields = List.copyOf(fields);
    }

    @Override
    protected HybridRetrieverBuilder clone(List<RetrieverSource> newChildRetrievers, List<QueryBuilder> newPreFilterQueryBuilders) {
        return null;
    }

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults, boolean explain) {
        return new RankDoc[0];
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {

    }
}
