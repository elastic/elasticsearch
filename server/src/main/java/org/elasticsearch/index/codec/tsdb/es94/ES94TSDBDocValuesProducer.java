/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es94;

import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.DocOffsetsCodec;
import org.elasticsearch.index.codec.tsdb.EntryFactory;
import org.elasticsearch.index.codec.tsdb.NumericFieldReader;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;

import java.io.IOException;

/**
 * ES94 doc values producer. Reads a {@link FieldDescriptor} from metadata to
 * reconstruct a {@link NumericDecoder} for each field, making the format
 * self-describing. Ordinal decoding delegates to {@link TSDBDocValuesEncoder}
 * for feature parity with ES819.
 */
final class ES94TSDBDocValuesProducer extends AbstractTSDBDocValuesProducer {

    private static final EntryFactory ENTRY_FACTORY = new EntryFactory() {
        @Override
        public NumericEntry createNumericEntry() {
            return new ES94NumericEntry();
        }

        @Override
        public SortedNumericEntry createSortedNumericEntry() {
            return new ES94SortedNumericEntry();
        }

        @Override
        public SortedEntry createSortedEntry() {
            return new PrefixPartitionedEntry();
        }
    };

    private final NumericCodecFactory numericCodecFactory;

    ES94TSDBDocValuesProducer(
        final SegmentReadState state,
        final String dataCodec,
        final String dataExtension,
        final String metaCodec,
        final String metaExtension,
        final TSDBDocValuesFormatConfig formatConfig,
        final DocOffsetsCodec.Decoder docOffsetsDecoder,
        final NumericCodecFactory numericCodecFactory
    ) throws IOException {
        super(state, dataCodec, dataExtension, metaCodec, metaExtension, formatConfig, docOffsetsDecoder, ENTRY_FACTORY);
        this.numericCodecFactory = numericCodecFactory;
    }

    private ES94TSDBDocValuesProducer(final ES94TSDBDocValuesProducer original) {
        super(original);
        this.numericCodecFactory = original.numericCodecFactory;
    }

    @Override
    protected NumericFieldReader createNumericFieldReader(final NumericEntry entry, int numericBlockSize) {
        final PipelineDescriptorEntry pipelineEntry = (PipelineDescriptorEntry) entry;
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
                        pipelineEntry.pipelineDescriptor(FieldDescriptor.read(meta));
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
                final TSDBDocValuesEncoder ordinalDecoder = new TSDBDocValuesEncoder(numericBlockSize);
                return new Decoder() {
                    private NumericBlockDecoder blockDecoder;

                    @Override
                    public void decodeBlock(final DataInput input, final long[] values, int count) throws IOException {
                        if (blockDecoder == null) {
                            final NumericDecoder decoder = numericCodecFactory.createDecoder(pipelineEntry.pipelineDescriptor());
                            blockDecoder = decoder.newBlockDecoder();
                        }
                        blockDecoder.decode(values, count, input);
                    }

                    @Override
                    public void decodeOrdinals(final DataInput input, final long[] values, int bitsPerOrd) throws IOException {
                        ordinalDecoder.decodeOrdinals(input, values, bitsPerOrd);
                    }
                };
            }
        };
    }

    @Override
    protected AbstractTSDBDocValuesProducer createMergeInstance() {
        return new ES94TSDBDocValuesProducer(this);
    }
}
