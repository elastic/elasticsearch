/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.apache.lucene.search.ScoreDoc;

public class IdentityScoreNormalizer extends ScoreNormalizer {

    public static final IdentityScoreNormalizer INSTANCE = new IdentityScoreNormalizer();

    public static final String NAME = "none";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public ScoreDoc[] normalizeScores(ScoreDoc[] docs) {
        return docs;
    }
}
