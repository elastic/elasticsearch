/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.NumericDocValues;

import java.io.IOException;

/**
 * Opens the ordinal doc values of one single-valued {@code Sorted} field (or the single-valued
 * representation of a {@code SortedSet} field).
 *
 * <p>Returned by {@link SortedOrdinalCodec#readOrdinalMeta} at segment-open time, bound to that field's
 * parsed metadata. The producer wraps the {@link NumericDocValues} into a {@code SortedDocValues} alongside
 * the terms dictionary it reads separately. One reader serves every producer instance of the segment,
 * merge instances included, so it must hold no mutable state.
 */
@FunctionalInterface
public interface SortedOrdinalReader {

    /**
     * Builds the per-doc ordinal doc values of the bound field.
     *
     * @param ctx    read state of the producer instance opening the doc values
     * @param maxOrd maximum ordinal value for this field
     * @return per-doc ordinal doc values
     */
    NumericDocValues ordinals(OrdinalReadContext ctx, long maxOrd) throws IOException;
}
