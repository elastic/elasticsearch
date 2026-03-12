/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.dfs;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;

import static org.elasticsearch.search.vectors.KnnScoreDocQueryBuilder.KNN_DFS_RESCORING_TOP_K_ON_SHARDS;

public class DfsKnnResults implements Writeable {
    private final String nestedPath;
    private final ScoreDoc[] scoreDocs;
    private final Float oversample;
    private final Integer k;

    public DfsKnnResults(String nestedPath, ScoreDoc[] scoreDocs, Float oversample, Integer k) {
        this.nestedPath = nestedPath;
        this.scoreDocs = scoreDocs;
        this.oversample = oversample;
        this.k = k;
    }

    public DfsKnnResults(StreamInput in) throws IOException {
        scoreDocs = in.readArray(Lucene::readScoreDoc, ScoreDoc[]::new);
        nestedPath = in.readOptionalString();
        if (in.getTransportVersion().supports(KNN_DFS_RESCORING_TOP_K_ON_SHARDS)) {
            oversample = in.readOptionalFloat();
            k = in.readOptionalVInt();
        } else {
            oversample = null;
            k = null;
        }
    }

    public String getNestedPath() {
        return nestedPath;
    }

    public ScoreDoc[] scoreDocs() {
        return scoreDocs;
    }

    public Float oversample() {
        return oversample;
    }

    public Integer k() {
        return k;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(Lucene::writeScoreDoc, scoreDocs);
        out.writeOptionalString(nestedPath);
        if (out.getTransportVersion().supports(KNN_DFS_RESCORING_TOP_K_ON_SHARDS)) {
            out.writeOptionalFloat(oversample);
            out.writeOptionalVInt(k);
        }
    }
}
