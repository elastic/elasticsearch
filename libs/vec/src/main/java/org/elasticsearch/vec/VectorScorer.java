/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec;

import java.io.IOException;

/** A scorer of vectors. */
public interface VectorScorer {

    /** Computes the score of the vectors at the given ordinals. */
    float score(int firstOrd, int secondOrd) throws IOException;

    /** The per-vector dimension size. */
    int dims();

    /** The maximum ordinal of vector this scorer can score. */
    int maxOrd();

}
