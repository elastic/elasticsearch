/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;

/**
 * Opens the ordinal doc values of one multi-valued {@code SortedSet} field.
 *
 * <p>Returned by {@link SortedSetOrdinalCodec#readOrdinalMeta} at segment-open time, bound to that field's
 * parsed metadata. The producer wraps the {@link SortedNumericDocValues} into a {@code SortedSetDocValues}
 * alongside the terms dictionary it reads separately. Mirrors {@link SortedOrdinalReader}, but the ordinal
 * stream carries multiple ordinals per document. One reader serves every producer instance of the segment,
 * merge instances included, so it must hold no mutable state.
 */
@FunctionalInterface
public interface SortedSetOrdinalReader {

    /**
     * Builds the per-doc ordinal doc values of the bound field.
     *
     * @param ctx    read state of the producer instance opening the doc values
     * @param maxOrd maximum ordinal value for this field
     * @return per-doc ordinal doc values
     */
    SortedNumericDocValues ordinals(OrdinalReadContext ctx, long maxOrd) throws IOException;
}
