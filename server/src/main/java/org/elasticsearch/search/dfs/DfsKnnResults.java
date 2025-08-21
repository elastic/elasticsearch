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

import static org.elasticsearch.TransportVersions.V_8_11_X;

public class DfsKnnResults implements Writeable {
    private final String nestedPath;
    private final ScoreDoc[] scoreDocs;

    public DfsKnnResults(String nestedPath, ScoreDoc[] scoreDocs) {
        this.nestedPath = nestedPath;
        this.scoreDocs = scoreDocs;
    }

    public DfsKnnResults(StreamInput in) throws IOException {
        scoreDocs = in.readArray(Lucene::readScoreDoc, ScoreDoc[]::new);
        if (in.getTransportVersion().onOrAfter(V_8_11_X)) {
            nestedPath = in.readOptionalString();
        } else {
            nestedPath = null;
        }
    }

    public String getNestedPath() {
        return nestedPath;
    }

    public ScoreDoc[] scoreDocs() {
        return scoreDocs;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(Lucene::writeScoreDoc, scoreDocs);
        if (out.getTransportVersion().onOrAfter(V_8_11_X)) {
            out.writeOptionalString(nestedPath);
        }
    }
}
