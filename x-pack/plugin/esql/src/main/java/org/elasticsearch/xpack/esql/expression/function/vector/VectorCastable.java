/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import java.util.Set;

/**
 * Implemented by functions whose arguments may need implicit casting to dense_vector
 * during analysis. Each implementation declares which child positions are eligible for
 * the cast; the Analyzer uses those indices to apply {@code castArgToDenseVector}
 * selectively, leaving other children (e.g. boolean conditions) untouched.
 */
public interface VectorCastable {
    /**
     * Returns the indices into {@code children()} that should be implicitly
     * cast to dense_vector when their current type is keyword (foldable hex
     * string) or numeric. Called during analysis before type resolution.
     * Implementations may examine their current children to make dynamic
     * decisions (e.g. only cast when a sibling is already dense_vector).
     */
    Set<Integer> denseVectorCastArgIndices();
}
