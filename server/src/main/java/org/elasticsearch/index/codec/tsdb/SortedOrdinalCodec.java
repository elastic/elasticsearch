/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;

import java.io.IOException;

/**
 * Encodes and decodes the ordinal stream of single-valued {@code Sorted} fields (and the single-valued
 * representation of {@code SortedSet} fields).
 *
 * <p>An instance is held by {@link AbstractTSDBDocValuesProducer} and
 * {@link AbstractTSDBDocValuesConsumer} for the lifetime of a segment and consulted once per field.
 * Symmetric with {@link SortedSetOrdinalCodec}; mirrors the way {@link NumericBlockCodec} injects the
 * numeric stream. A codec instance is shared across segments and merge threads, so it must be immutable;
 * {@link #createWriter} should return a fresh writer per call so each field owns its own encoding state.
 */
public interface SortedOrdinalCodec {

    /**
     * Returns a writer that encodes the ordinal stream of a sorted field in this segment.
     *
     * @param ctx segment-scoped write state shared by every field in this segment
     * @return    the sorted ordinal field writer
     */
    SortedOrdinalWriter createWriter(NumericWriteContext ctx);

    /**
     * Parses one field's ordinal metadata at segment-open time and returns the reader bound to it.
     *
     * <p>What the producer needs from every layout, the value and document counts and the empty-field
     * marker, is written into {@code entry}; anything else the layout needs to open the doc values stays
     * with the returned reader, which the producer keeps next to the entry for the lifetime of the segment
     * and shares with its merge instances. The reader therefore must hold no mutable state.
     *
     * @param ctx               segment-scoped read state shared by every field in this segment
     * @param meta              segment metadata input positioned at this field's header
     * @param entry             entry to populate with the metadata the producer relies on
     * @param numericBlockShift block shift used to size the per-field block index
     * @return                  the reader that opens this field's ordinal doc values
     */
    SortedOrdinalReader readOrdinalMeta(NumericReadContext ctx, IndexInput meta, NumericEntry entry, int numericBlockShift)
        throws IOException;
}
