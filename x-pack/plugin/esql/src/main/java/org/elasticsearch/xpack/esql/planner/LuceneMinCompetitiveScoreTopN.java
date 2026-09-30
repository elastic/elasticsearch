/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.lucene.query.MinCompetitiveScore;
import org.elasticsearch.compute.operator.topn.SharedGlobalTopK;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.core.Nullable;

/**
 * Wiring for {@code SORT _score DESC | LIMIT N} over Lucene when the TopN could not be pushed to
 * Lucene, usually because of a filter that has to run in the compute engine. The {@code TopNOperator}s
 * publish the {@code _score} of their least competitive row into {@code supplier} and the
 * {@code LuceneSourceOperator}s read it back through {@link MinCompetitiveScore} and hand it to
 * Lucene's scorers so they can skip documents that can't make the top N.
 * <p>
 * This is the {@code _score} counterpart of {@link LuceneMinCompetitiveTimestampTopN}. It is a separate
 * type because it feeds the scorer rather than a {@code competitiveIterator} built from a field.
 *
 * @param globalTopK non-null when drivers merge their heaps into one node-wide top N to publish a
 *                   tighter bound sooner. See {@link SharedGlobalTopK}.
 */
public record LuceneMinCompetitiveScoreTopN(SharedMinCompetitive.Supplier supplier, @Nullable SharedGlobalTopK.Supplier globalTopK) {
    public MinCompetitiveScore.Factory minCompetitiveScoreFactory() {
        return new MinCompetitiveScore.Factory(supplier);
    }
}
