/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rrf;

import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.rank.RankShardResult;

import java.io.IOException;
import java.util.List;

public class RRFRankShardResult extends RankShardResult {

    private final List<TopDocs> topDocs;

    public RRFRankShardResult(List<TopDocs> topDocs) {
        this.topDocs = topDocs;
    }

    public RRFRankShardResult(StreamInput in) throws IOException {
        topDocs = in.readList(Lucene::readOnlyTopDocs);
    }

    @Override
    public String getName() {
        return RRFRankContextBuilder.NAME.getPreferredName();
    }

    public List<TopDocs> getTopDocs() {
        return topDocs;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeCollection(topDocs, Lucene::writeOnlyTopDocs);
    }
}
