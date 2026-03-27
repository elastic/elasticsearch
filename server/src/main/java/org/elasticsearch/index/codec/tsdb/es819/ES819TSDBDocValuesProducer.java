/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.DocOffsetsCodec;
import org.elasticsearch.index.codec.tsdb.NumericFieldReader;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig;

import java.io.IOException;

/**
 * ES819 doc values producer. Delegates all shared wire-format reading logic to
 * {@link AbstractTSDBDocValuesProducer} and provides the ES819-specific numeric
 * decoding strategy via {@link TSDBDocValuesEncoder}.
 */
final class ES819TSDBDocValuesProducer extends AbstractTSDBDocValuesProducer {

    ES819TSDBDocValuesProducer(
        final SegmentReadState state,
        final String dataCodec,
        final String dataExtension,
        final String metaCodec,
        final String metaExtension,
        final TSDBDocValuesFormatConfig formatConfig,
        final DocOffsetsCodec.Decoder docOffsetsDecoder
    ) throws IOException {
        super(state, dataCodec, dataExtension, metaCodec, metaExtension, formatConfig, docOffsetsDecoder);
    }

    private ES819TSDBDocValuesProducer(final ES819TSDBDocValuesProducer original) {
        super(original);
    }

    @Override
    protected NumericFieldReader createNumericFieldReader(final NumericEntry entry, int numericBlockSize) {
        return new NumericFieldReader() {
            @Override
            public void readField(final IndexInput meta, final NumericEntry e, int numericBlockShift) throws IOException {
                e.numValues = meta.readLong();
                e.numDocsWithField = meta.readInt();
                if (e.numValues > 0) {
                    final int indexBlockShift = meta.readInt();
                    if (indexBlockShift == AbstractTSDBDocValuesConsumer.INDEX_SINGLE_ORDINAL) {
                        // single ordinal, no block index
                    } else if (indexBlockShift == AbstractTSDBDocValuesConsumer.INDEX_ORDINAL_RANGE) {
                        final int numOrds = meta.readVInt();
                        final int blockShift = meta.readByte();
                        e.sortedOrdinals = DirectMonotonicReader.loadMeta(meta, numOrds + 1, blockShift);
                    } else {
                        e.indexMeta = DirectMonotonicReader.loadMeta(meta, 1 + ((e.numValues - 1) >>> numericBlockShift), indexBlockShift);
                    }
                    e.indexOffset = meta.readLong();
                    e.indexLength = meta.readLong();
                    e.valuesOffset = meta.readLong();
                    e.valuesLength = meta.readLong();
                }
                e.docsWithFieldOffset = meta.readLong();
                e.docsWithFieldLength = meta.readLong();
                e.jumpTableEntryCount = meta.readShort();
                e.denseRankPower = meta.readByte();
            }

            @Override
            public Decoder decoder() {
                final TSDBDocValuesEncoder decoder = new TSDBDocValuesEncoder(numericBlockSize);
                return new Decoder() {
                    @Override
                    public void decodeBlock(final DataInput input, final long[] values, int count) throws IOException {
                        decoder.decode(input, values);
                    }

                    @Override
                    public void decodeOrdinals(final DataInput input, final long[] values, int bitsPerOrd) throws IOException {
                        decoder.decodeOrdinals(input, values, bitsPerOrd);
                    }
                };
            }
        };
    }

    @Override
    protected AbstractTSDBDocValuesProducer createMergeInstance() {
        return new ES819TSDBDocValuesProducer(this);
    }
}
