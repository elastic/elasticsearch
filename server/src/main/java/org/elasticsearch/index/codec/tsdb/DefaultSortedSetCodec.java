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
 * Baseline {@link SortedSetOrdinalCodec} that encodes and decodes the sorted-set ordinal stream with the
 * block layout of an {@link OrdinalBlockCodec}.
 *
 * <p>The writer delegates to the block codec's {@link OrdinalFieldWriter}, so the on-disk bytes are
 * identical to the ordinal stream this format wrote before the sorted/sorted-set split. On read,
 * {@link SortedSetOrdinalReader#readOrdinalMeta} delegates to the block codec's
 * {@link OrdinalFieldReader}, and {@link SortedSetOrdinalReader#ordinals} delegates to the
 * producer's shared multi-valued ordinal doc values construction, driving it with the block codec's
 * per-block decoder. This is the fallback that a run-table codec composes for fields it does not
 * encode itself.
 */
public class DefaultSortedSetCodec implements SortedSetOrdinalCodec {

    private final OrdinalBlockCodec ordinalBlockCodec;

    /**
     * @param ordinalBlockCodec the block-level ordinal encoding this format wrote before the
     *                          sorted/sorted-set split
     */
    public DefaultSortedSetCodec(final OrdinalBlockCodec ordinalBlockCodec) {
        this.ordinalBlockCodec = ordinalBlockCodec;
    }

    @Override
    public SortedSetOrdinalWriter createWriter(final NumericWriteContext ctx) {
        // writeOrdinals and OrdinalFieldWriter#writeFieldEntry share a signature.
        return ordinalBlockCodec.createWriter(ctx)::writeFieldEntry;
    }

    @Override
    public SortedSetOrdinalReader createReader(final NumericReadContext ctx, final IndexInput data, int maxDoc) {
        return new DefaultSortedSetOrdinalReader(ordinalBlockCodec.createReader(ctx), ctx.producer());
    }

    private static final class DefaultSortedSetOrdinalReader implements SortedSetOrdinalReader {

        private final OrdinalFieldReader ordinalFieldReader;
        private final AbstractTSDBDocValuesProducer producer;

        DefaultSortedSetOrdinalReader(final OrdinalFieldReader ordinalFieldReader, final AbstractTSDBDocValuesProducer producer) {
            this.ordinalFieldReader = ordinalFieldReader;
            this.producer = producer;
        }

        @Override
        public void readOrdinalMeta(final IndexInput meta, final NumericEntry entry, int numericBlockShift) throws IOException {
            ordinalFieldReader.readFieldEntry(meta, entry, numericBlockShift);
        }

        @Override
        public SortedNumericDocValues ordinals(final SortedNumericEntry entry, long maxOrd) throws IOException {
            return producer.buildSortedSetOrdinals(entry, maxOrd, ordinalFieldReader.decoder(entry.blockSize));
        }
    }
}
