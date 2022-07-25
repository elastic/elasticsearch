/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.dfs;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;

public class DfsKnnResults implements Writeable {
    private final ScoreDoc[] scoreDocs;

    public DfsKnnResults(ScoreDoc[] scoreDocs) {
        this.scoreDocs = scoreDocs;
    }

    public DfsKnnResults(StreamInput in) throws IOException {
        scoreDocs = in.readArray(Lucene::readScoreDoc, ScoreDoc[]::new);
    }

    public ScoreDoc[] scoreDocs() {
        return scoreDocs;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(Lucene::writeScoreDoc, scoreDocs);
    }
}
