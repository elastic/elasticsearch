/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import org.elasticsearch.core.Releasable;

/**
 * Values loaded for one selection, compacted into selection order. {@code value(0)} is the first selected position,
 * {@code value(1)} the next, and so on. The {@link SelectionMask} passed to {@link SelectionBatch#load} maps those
 * ordinals back onto the batch.
 * <p>
 * The caller closes the column before the batch is released and does not retain it, or a block loaded from it, for a
 * later batch. Each load returns its own column, so two branches can read the same field. A real adapter accounts the
 * block to the circuit breaker while loading and releases it on {@link #close()}.
 */
public interface Column extends Releasable {
    Object value(int selectionOrdinal);

    @Override
    default void close() {}
}
