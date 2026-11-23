/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * A batch of child rows. {@link #parentPosition(int)} is an index in the parent row space, not a child source id, and it
 * stays stable when a parent's children are split across batches.
 */
public interface ChildBatch extends SelectionBatch {
    int parentPosition(int position);
}
