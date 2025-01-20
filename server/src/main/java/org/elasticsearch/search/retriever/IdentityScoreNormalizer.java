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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class IdentityScoreNormalizer extends ScoreNormalizer {

    public static final IdentityScoreNormalizer INSTANCE = new IdentityScoreNormalizer();

    public static final String NAME = "none";

    public static final ConstructingObjectParser<IdentityScoreNormalizer, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
        if (args.length != 0) {
            throw new IllegalArgumentException("[IdentityScoreNormalizer] does not accept any arguments");
        }
        return new IdentityScoreNormalizer();
    });

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public ScoreDoc[] normalizeScores(ScoreDoc[] docs) {
        return docs;
    }

    public static IdentityScoreNormalizer fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        // no-op
    }
}
