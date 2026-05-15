/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.elasticsearch.simdvec.BaseVectorizationTests;

import java.util.List;

public class GenericFlatVectorScorerTest extends BaseVectorizationTests {

    // we're just testing arrays here, don't need to go into directorys/inputs/etc

    private final FlatVectorsScorer scorer;

    public GenericFlatVectorScorerTest(FlatVectorsScorer scorer) {
        this.scorer = scorer;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.<Object[]>of(
            new Object[] {
                defaultProvider().getVectorScorerFactory().newGenericFlatVectorScorer(),
                nativeProvider().getVectorScorerFactory().newGenericFlatVectorScorer() }
        );
    }
}
