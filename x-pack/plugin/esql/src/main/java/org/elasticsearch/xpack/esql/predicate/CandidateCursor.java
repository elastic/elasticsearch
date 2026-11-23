/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/** A source-produced stream of batches that may contain matches. {@link #nextBatch()} returns null when the stream ends. */
public interface CandidateCursor {
    SelectionBatch nextBatch();
}
