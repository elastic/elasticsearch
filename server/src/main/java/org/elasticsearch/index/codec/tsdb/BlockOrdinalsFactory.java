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
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.SortedNumericEntry;

import java.io.IOException;

/**
 * Builds the per-doc ordinal doc values of a field stored in the blocked ordinal layout: the layout an
 * {@link OrdinalBlockCodec} encodes block by block and that {@link AbstractTSDBDocValuesProducer} knows how
 * to iterate in its dense, sparse and range-encoded variants.
 *
 * <p>Implemented by the producer and handed to ordinal readers through {@link OrdinalReadContext}, so a
 * {@link SortedOrdinalReader} or {@link SortedSetOrdinalReader} can serve the blocked layout without
 * holding a reference to the producer. The reader supplies the {@code decoder} its
 * {@link OrdinalFieldReader} produces for the field's block size; everything else about the blocked
 * layout stays inside the producer.
 */
public interface BlockOrdinalsFactory {

    /**
     * Builds the single-valued ordinal doc values of a field in the blocked layout.
     *
     * @param entry   the field entry populated at segment-open time
     * @param maxOrd  the number of terms of the field
     * @param decoder the per-block ordinal decoder for the entry's block size
     */
    NumericDocValues sorted(NumericEntry entry, long maxOrd, OrdinalFieldReader.Decoder decoder) throws IOException;

    /**
     * Builds the multi-valued ordinal doc values of a field in the blocked layout.
     *
     * @param entry   the field entry populated at segment-open time
     * @param maxOrd  the number of terms of the field
     * @param decoder the per-block ordinal decoder for the entry's block size
     */
    SortedNumericDocValues sortedSet(SortedNumericEntry entry, long maxOrd, OrdinalFieldReader.Decoder decoder) throws IOException;
}
