/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;

import java.io.IOException;

/**
 * Returns the a constant for every search hit against which it is run.
 */
public class ScoreExtractor implements HitExtractor {
    public static final HitExtractor INSTANCE = new ScoreExtractor();
    /**
     * Stands for {@code score}. We try to use short names for {@link HitExtractor}s
     * to save a few bytes when when we send them back to the user.
     */
    static final String NAME = "sc";

    private ScoreExtractor() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Nothing to write
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object extract(SearchHit hit) {
        return hit.getScore();
    }

    @Override
    public String hitName() {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return 31;
    }

    @Override
    public String toString() {
        return "SCORE";
    }
}
