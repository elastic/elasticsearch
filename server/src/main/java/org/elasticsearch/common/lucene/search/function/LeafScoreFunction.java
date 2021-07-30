/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.search.Explanation;

import java.io.IOException;

/** Per-leaf {@link ScoreFunction}. */
public abstract class LeafScoreFunction {

    public abstract double score(int docId, float subQueryScore) throws IOException;

    public abstract Explanation explainScore(int docId, Explanation subQueryScore) throws IOException;

}
