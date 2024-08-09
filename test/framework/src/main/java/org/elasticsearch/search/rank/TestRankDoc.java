/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class TestRankDoc extends RankDoc {

    public static final String NAME = "test_rank_doc";

    public TestRankDoc(int doc, float score, int shardIndex) {
        super(doc, score, shardIndex);
    }

    public TestRankDoc(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Explanation explain() {
        throw new UnsupportedOperationException("not supported for {" + getClass() + "}");
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        // do nothing
    }

    @Override
    public boolean doEquals(RankDoc rd) {
        return true;
    }

    @Override
    public int doHashCode() {
        return 0;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) {
        // no-op
    }
}
