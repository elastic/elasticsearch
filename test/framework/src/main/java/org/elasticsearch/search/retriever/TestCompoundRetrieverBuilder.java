/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.UnknownServiceException;
import java.util.ArrayList;
import java.util.List;

public class TestCompoundRetrieverBuilder extends CompoundRetrieverBuilder<TestCompoundRetrieverBuilder> {

    public static final String NAME = "test_compound_retriever_builder";

    public TestCompoundRetrieverBuilder(int rankWindowSize) {
        this(new ArrayList<>(), rankWindowSize);
    }

    TestCompoundRetrieverBuilder(List<RetrieverSource> childRetrievers, int rankWindowSize) {
        super(childRetrievers, rankWindowSize);
    }

    @Override
    protected TestCompoundRetrieverBuilder clone(List<RetrieverSource> newChildRetrievers) {
        return new TestCompoundRetrieverBuilder(newChildRetrievers, rankWindowSize);
    }

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults) {
        return new RankDoc[0];
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnknownServiceException("should not be called");
    }
}
