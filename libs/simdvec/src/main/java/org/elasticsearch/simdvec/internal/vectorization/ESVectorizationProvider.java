/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;

import java.io.IOException;
import java.util.Objects;

public abstract class ESVectorizationProvider {

    public static ESVectorizationProvider getInstance() {
        return Objects.requireNonNull(
            ESVectorizationProvider.Holder.INSTANCE,
            "call to getInstance() from subclass of VectorizationProvider"
        );
    }

    ESVectorizationProvider() {}

    public abstract ESVectorUtilSupport getVectorUtilSupport();

    /** Create a new {@link ES91OSQVectorsScorer} for the given {@link IndexInput}. */
    public abstract ES91OSQVectorsScorer newES91OSQVectorsScorer(IndexInput input, int dimension) throws IOException;

    // visible for tests
    static ESVectorizationProvider lookup(boolean testMode) {
        return new DefaultESVectorizationProvider();
    }

    /** This static holder class prevents classloading deadlock. */
    private static final class Holder {
        private Holder() {}

        static final ESVectorizationProvider INSTANCE = lookup(false);
    }
}
