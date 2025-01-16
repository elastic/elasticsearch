/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.normalizer;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A no-op {@link ScoreNormalizer} that does not modify the scores.
 */
public abstract class ScoreNormalizer implements ToXContent {

    public static ScoreNormalizer valueOf(String normalizer) {
        if (MinMaxScoreNormalizer.NAME.equalsIgnoreCase(normalizer)) {
            return new MinMaxScoreNormalizer();
        } else if (IdentityScoreNormalizer.NAME.equalsIgnoreCase(normalizer)) {
            return new IdentityScoreNormalizer();

        } else {
            throw new IllegalArgumentException("Unknown normalizer [" + normalizer + "]");
        }
    }

    protected abstract void doToXContent(XContentBuilder builder, Params params) throws IOException;

    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(getName());
        doToXContent(builder, params);
        builder.endObject();
        builder.endObject();

        return builder;
    }

    public abstract String getName();

    public abstract ScoreDoc[] normalizeScores(ScoreDoc[] docs);
}
