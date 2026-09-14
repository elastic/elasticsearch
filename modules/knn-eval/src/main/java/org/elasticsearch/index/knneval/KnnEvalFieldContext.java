/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

import org.elasticsearch.core.Nullable;

/** Everything derived from the field's mapping and the cluster, each part {@code null} when its source was unavailable. */
record KnnEvalFieldContext(
    @Nullable KnnEvalFidelity fidelity,
    @Nullable KnnEvalRescore rescore,
    @Nullable KnnEvalEnvironment.FieldSummary field,
    @Nullable KnnEvalEnvironment environment
) {
    static final KnnEvalFieldContext EMPTY = new KnnEvalFieldContext(null, null, null, null);

    KnnEvalFieldContext withEnvironment(KnnEvalEnvironment environment) {
        return new KnnEvalFieldContext(fidelity, rescore, field, environment);
    }
}
