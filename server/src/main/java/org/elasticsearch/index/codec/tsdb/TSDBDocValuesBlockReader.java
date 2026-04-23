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
import org.apache.lucene.util.packed.DirectMonotonicReader;

import java.io.IOException;

/**
 * Shared metadata parsing for TSDB doc values fields.
 *
 * <p>Both numeric and ordinal fields use the same on-disk metadata layout: value counts,
 * block index, value offsets, and DISI metadata. This class owns that shared parsing so
 * {@link TSDBNumericFieldReader} and {@link TSDBOrdinalFieldReader} can delegate to a single
 * implementation without duplicating code.
 *
 * @see TSDBDocValuesBlockWriter
 */
final class TSDBDocValuesBlockReader {

    /**
     * Parses the field metadata from {@code meta} into {@code entry}.
     *
     * @param meta              segment metadata input positioned at this field's header
     * @param entry             entry to populate with the parsed metadata
     * @param numericBlockShift block shift used to size the per-field block index
     */
    void readFieldEntry(final IndexInput meta, final AbstractTSDBDocValuesProducer.NumericEntry entry, int numericBlockShift)
        throws IOException {
        entry.numValues = meta.readLong();
        entry.numDocsWithField = meta.readInt();
        if (entry.numValues > 0) {
            final int indexBlockShift = meta.readInt();
            if (indexBlockShift == AbstractTSDBDocValuesConsumer.INDEX_SINGLE_ORDINAL) {
                // single ordinal, no block index
            } else if (indexBlockShift == AbstractTSDBDocValuesConsumer.INDEX_ORDINAL_RANGE) {
                final int numOrds = meta.readVInt();
                final int blockShift = meta.readByte();
                entry.sortedOrdinals = DirectMonotonicReader.loadMeta(meta, numOrds + 1, blockShift);
            } else {
                entry.indexMeta = DirectMonotonicReader.loadMeta(meta, 1 + ((entry.numValues - 1) >>> numericBlockShift), indexBlockShift);
            }
            entry.indexOffset = meta.readLong();
            entry.indexLength = meta.readLong();
            entry.valuesOffset = meta.readLong();
            entry.valuesLength = meta.readLong();
        }
        entry.docsWithFieldOffset = meta.readLong();
        entry.docsWithFieldLength = meta.readLong();
        entry.jumpTableEntryCount = meta.readShort();
        entry.denseRankPower = meta.readByte();
    }
}
