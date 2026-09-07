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
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.SortedNumericEntry;

import java.io.IOException;

/**
 * Reads the ordinal stream of a multi-valued {@code SortedSet} field from a segment.
 *
 * <p>Created by
 * {@link SortedSetOrdinalCodec#createReader(NumericReadContext, org.apache.lucene.store.IndexInput, int)}.
 * {@link #readOrdinalMeta} runs once per field at segment-open time to parse the field metadata;
 * {@link #ordinals} builds the per-doc ordinal {@link SortedNumericDocValues} that the producer
 * wraps into a {@code SortedSetDocValues} alongside the terms dictionary it reads separately.
 * Mirrors {@link SortedOrdinalReader}, but the ordinal stream carries multiple ordinals per
 * document.
 */
public interface SortedSetOrdinalReader {

    /**
     * Parses the field metadata into {@code entry}.
     *
     * @param meta              segment metadata input positioned at this field's header
     * @param entry             entry to populate with the parsed metadata
     * @param numericBlockShift block shift used to size the per-field block index
     */
    void readOrdinalMeta(IndexInput meta, NumericEntry entry, int numericBlockShift) throws IOException;

    /**
     * Builds the per-doc ordinal doc values for a field parsed by {@link #readOrdinalMeta}.
     *
     * @param entry  the field entry populated by {@link #readOrdinalMeta}
     * @param maxOrd maximum ordinal value for this field
     * @return per-doc ordinal doc values
     */
    SortedNumericDocValues ordinals(SortedNumericEntry entry, long maxOrd) throws IOException;

    /**
     * Returns a run-granularity view of the field parsed by {@link #readOrdinalMeta}, or {@code null} if this
     * reader did not store the field run-table encoded. Segment merge uses it to read a source field once per run
     * instead of once per doc; a {@code null} return signals the field must fall back to the per-doc merge.
     */
    default SortedSetRunView runs(SortedNumericEntry entry, long maxOrd) throws IOException {
        return null;
    }
}
