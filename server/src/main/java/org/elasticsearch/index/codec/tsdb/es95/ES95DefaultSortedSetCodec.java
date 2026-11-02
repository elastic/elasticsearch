/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.SortedNumericEntry;
import org.elasticsearch.index.codec.tsdb.BlockOrdinalsFactory;
import org.elasticsearch.index.codec.tsdb.MultiValueAddresses;
import org.elasticsearch.index.codec.tsdb.NumericReadContext;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.OrdinalBlockCodec;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldReader;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldWriter;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalCodec;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalReader;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalWriter;

import java.io.IOException;

/**
 * Baseline {@link SortedSetOrdinalCodec} that encodes and decodes the sorted-set ordinal stream with the
 * block layout of an {@link OrdinalBlockCodec}, followed by the per-doc addresses table of
 * {@link MultiValueAddresses}.
 *
 * <p>The writer delegates the stream to the block codec's {@link OrdinalFieldWriter} and appends the
 * addresses table, so the on-disk bytes are identical to what this format wrote before the
 * sorted/sorted-set split. On read, {@link #readOrdinalMeta} delegates to the block codec's
 * {@link OrdinalFieldReader} and then reads the addresses metadata, and the returned
 * {@link SortedSetOrdinalReader} asks the {@link BlockOrdinalsFactory} of the read context to build the
 * multi-valued doc values, driving it with the block codec's per-block decoder. This is the fallback that
 * a run-table codec composes for fields it does not encode itself.
 */
public final class ES95DefaultSortedSetCodec implements SortedSetOrdinalCodec {

    private final OrdinalBlockCodec ordinalBlockCodec;

    /**
     * @param ordinalBlockCodec the block-level ordinal encoding this format wrote before the
     *                          sorted/sorted-set split
     */
    public ES95DefaultSortedSetCodec(final OrdinalBlockCodec ordinalBlockCodec) {
        this.ordinalBlockCodec = ordinalBlockCodec;
    }

    @Override
    public SortedSetOrdinalWriter createWriter(final NumericWriteContext ctx) {
        final OrdinalFieldWriter writer = ordinalBlockCodec.createWriter(ctx);
        // multi-valued: the consumer attaches no sorted field observer
        return (field, values, maxOrd) -> MultiValueAddresses.write(
            ctx,
            field,
            values,
            accumulator -> writer.writeFieldEntry(field, values, maxOrd, accumulator != null ? accumulator::addDoc : null, null)
        );
    }

    @Override
    public SortedSetOrdinalReader readOrdinalMeta(
        final NumericReadContext ctx,
        final IndexInput meta,
        final SortedNumericEntry entry,
        int numericBlockShift
    ) throws IOException {
        ordinalBlockCodec.createReader(ctx).readFieldEntry(meta, entry, numericBlockShift);
        MultiValueAddresses.readMeta(meta, entry);
        // The block codec's decoders carry per-block buffers, so each opened doc values gets its own.
        // TODO: assuming OrdinalFieldReader is stateless for every OrdinalBlockCodec can we create it
        // once above and capture it here, so each open only calls decoder()?
        return (readCtx, maxOrd) -> readCtx.blockOrdinals()
            .sortedSet(entry, maxOrd, ordinalBlockCodec.createReader(ctx).decoder(entry.blockSize));
    }
}
