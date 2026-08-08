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
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;

import java.io.IOException;

/**
 * Reads the ordinal stream of a single-valued {@code Sorted} field (and the single-valued
 * representation of a {@code SortedSet} field) from a segment.
 *
 * <p>Created by {@link SortedOrdinalCodec#createReader(NumericReadContext, org.apache.lucene.store.IndexInput, int)}.
 * {@link #readOrdinalMeta} runs once per field at segment-open time to parse the field metadata into
 * a {@link NumericEntry}; {@link #ordinals} builds the per-doc ordinal {@link NumericDocValues} that
 * the producer wraps into a {@code SortedDocValues} alongside the terms dictionary it reads
 * separately.
 */
public interface SortedOrdinalReader {

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
    NumericDocValues ordinals(NumericEntry entry, long maxOrd) throws IOException;
}
