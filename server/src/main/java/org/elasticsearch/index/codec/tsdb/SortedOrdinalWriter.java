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

import java.io.IOException;

/**
 * Writes the ordinal stream of a single-valued {@code Sorted} field (and the single-valued
 * representation of a {@code SortedSet} field) to a segment.
 *
 * <p>Created by {@link SortedOrdinalCodec#createWriter(NumericWriteContext)} once per field; the shared
 * consumer wire-format code drives {@link #writeOrdinals} to emit the per-field metadata and the
 * encoded ordinal blocks.
 */
public interface SortedOrdinalWriter {

    /**
     * Writes one sorted ordinal field and returns its statistics.
     *
     * @param field                 field being written
     * @param values                source of doc values for this field
     * @param maxOrd                maximum ordinal value for this field
     * @param docValueCountConsumer receives the per-doc value count for offset tracking, or
     *                              {@code null} when offsets are not needed
     * @param sortedFieldObserver   receives {@code (docId, ord)} pairs during the doc pass, or
     *                              {@code null} when no observer is attached
     * @return the field's doc value count statistics
     */
    DocValueFieldCountStats writeOrdinals(
        FieldInfo field,
        TsdbDocValuesProducer values,
        long maxOrd,
        AbstractTSDBDocValuesConsumer.DocValueCountConsumer docValueCountConsumer,
        SortedFieldObserver sortedFieldObserver
    ) throws IOException;
}
