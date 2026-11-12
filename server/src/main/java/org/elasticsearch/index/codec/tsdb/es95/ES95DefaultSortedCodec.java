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
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;
import org.elasticsearch.index.codec.tsdb.BlockOrdinalsFactory;
import org.elasticsearch.index.codec.tsdb.NumericReadContext;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.OrdinalBlockCodec;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldReader;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldWriter;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalCodec;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalReader;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalWriter;

import java.io.IOException;

/**
 * Baseline {@link SortedOrdinalCodec} that encodes and decodes the sorted ordinal stream with the block
 * layout of an {@link OrdinalBlockCodec}.
 *
 * <p>The writer delegates to the block codec's {@link OrdinalFieldWriter}, so the on-disk bytes are
 * identical to the ordinal stream this format wrote before the sorted/sorted-set split. On read,
 * {@link #readOrdinalMeta} delegates to the block codec's {@link OrdinalFieldReader}, and the returned
 * {@link SortedOrdinalReader} asks the {@link BlockOrdinalsFactory} of the read context to build the doc
 * values, driving it with the block codec's per-block decoder. This is the fallback that a run-table codec
 * composes for fields it does not encode itself.
 */
public final class ES95DefaultSortedCodec implements SortedOrdinalCodec {

    private final OrdinalBlockCodec ordinalBlockCodec;

    /**
     * @param ordinalBlockCodec the block-level ordinal encoding this format wrote before the
     *                          sorted/sorted-set split
     */
    public ES95DefaultSortedCodec(final OrdinalBlockCodec ordinalBlockCodec) {
        this.ordinalBlockCodec = ordinalBlockCodec;
    }

    @Override
    public SortedOrdinalWriter createWriter(final NumericWriteContext ctx) {
        final OrdinalFieldWriter writer = ordinalBlockCodec.createWriter(ctx);
        // single-valued: no per-doc value count to report
        return (field, values, maxOrd, sortedFieldObserver) -> writer.writeFieldEntry(field, values, maxOrd, null, sortedFieldObserver);
    }

    @Override
    public SortedOrdinalReader readOrdinalMeta(
        final NumericReadContext ctx,
        final IndexInput meta,
        final NumericEntry entry,
        int numericBlockShift
    ) throws IOException {
        ordinalBlockCodec.createReader(ctx).readFieldEntry(meta, entry, numericBlockShift);
        // The block codec's decoders carry per-block buffers, so each opened doc values gets its own.
        // TODO: assuming OrdinalFieldReader is stateless for every OrdinalBlockCodec can we create it
        // once above and capture it here, so each open only calls decoder()?
        return (readCtx, maxOrd) -> readCtx.blockOrdinals()
            .sorted(entry, maxOrd, ordinalBlockCodec.createReader(ctx).decoder(entry.blockSize));
    }
}
