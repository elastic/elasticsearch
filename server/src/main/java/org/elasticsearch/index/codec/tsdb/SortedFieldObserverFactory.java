/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.FieldInfo;

/**
 * Factory for creating {@link SortedFieldObserver} instances during sorted field writing.
 *
 * <p>The factory is injected into {@link AbstractTSDBDocValuesConsumer} at construction time.
 * It is called once per sorted field to obtain an observer for that field. Implementations
 * decide based on field properties and format configuration whether to return a real
 * observer or {@link SortedFieldObserver#NOOP}.
 *
 * @see SortedFieldObserver
 */
@FunctionalInterface
public interface SortedFieldObserverFactory {

    /** Factory that always returns {@link SortedFieldObserver#NOOP}. */
    SortedFieldObserverFactory NOOP = field -> SortedFieldObserver.NOOP;

    /**
     * Creates an observer for the given sorted field.
     *
     * @param field the field being written
     * @return an observer for this field, or {@link SortedFieldObserver#NOOP} if no observation is needed
     */
    SortedFieldObserver create(FieldInfo field);
}
