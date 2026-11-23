/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * What a source can do with a predicate. Producing candidates and refining positions already in a batch are different
 * operations: a scan can do one, the other, or both.
 */
public enum SourceOperation {
    /** Build a new stream of batches that may contain matches. */
    PRODUCE,
    /** Evaluate the predicate over positions that are already in a batch. */
    REFINE
}
