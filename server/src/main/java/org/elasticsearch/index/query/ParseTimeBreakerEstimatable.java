/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

/**
 * Optional interface for {@link QueryBuilder} implementations that do not extend
 * {@link AbstractQueryBuilder} but carry variable-length payload (e.g. field names)
 * that should be counted toward the parse-time circuit-breaker estimate.
 *
 * Implementations return the number of bytes the query builder is estimated to
 * occupy on heap immediately after parsing, using the same conventions as
 * {@link AbstractQueryBuilder#parseTimeBreakerEstimate()}.
 */
interface ParseTimeBreakerEstimatable {
    long parseTimeBreakerEstimate();
}
