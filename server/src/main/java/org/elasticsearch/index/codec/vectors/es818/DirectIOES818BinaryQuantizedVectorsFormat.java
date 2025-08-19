/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;

/**
 * A variant of
 */
public class DirectIOES818BinaryQuantizedVectorsFormat extends ES818BinaryQuantizedVectorsFormat {

    public static final String NAME = "DirectIOES818BinaryQuantizedVectorsFormat";

    public DirectIOES818BinaryQuantizedVectorsFormat() {
        super(NAME, new DirectIOLucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer()));
    }
}
