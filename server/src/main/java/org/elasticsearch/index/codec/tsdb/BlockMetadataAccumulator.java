/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.elasticsearch.core.IOUtils;

import java.io.Closeable;
import java.io.IOException;

final class BlockMetadataAccumulator implements Closeable {

    private final DelayedOffsetAccumulator blockAddressAcc;
    private final DelayedOffsetAccumulator blockDocRangeAcc;

    /**
     * Creates a new accumulator for block metadata (addresses and doc ranges).
     *
     * @param dir the directory for temporary files
     * @param context the IO context
     * @param data the data output
     * @param addressesStart the starting offset for block addresses
     * @param metaCodec the codec name used in temp file headers
     * @param versionCurrent the codec version used in temp file headers
     * @param directMonotonicBlockShift the block shift for address/offset encoding via {@link DirectMonotonicWriter}
     */
    BlockMetadataAccumulator(
        Directory dir,
        IOContext context,
        IndexOutput data,
        long addressesStart,
        String metaCodec,
        int versionCurrent,
        int directMonotonicBlockShift
    ) throws IOException {
        boolean success = false;
        try {
            blockDocRangeAcc = new DelayedOffsetAccumulator(
                dir,
                context,
                data,
                "block-doc-ranges",
                0,
                metaCodec,
                versionCurrent,
                directMonotonicBlockShift
            );
            blockAddressAcc = new DelayedOffsetAccumulator(
                dir,
                context,
                data,
                "block-addresses",
                addressesStart,
                metaCodec,
                versionCurrent,
                directMonotonicBlockShift
            );
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this); // self-close because constructor caller can't
            }
        }
    }

    public void addDoc(long numDocsInBlock, long blockLenInBytes) throws IOException {
        blockDocRangeAcc.addDoc(numDocsInBlock);
        blockAddressAcc.addDoc(blockLenInBytes);
    }

    public void build(IndexOutput meta, IndexOutput data) throws IOException {
        long dataAddressesStart = data.getFilePointer();
        blockAddressAcc.build(meta, data);
        long dataDocRangeStart = data.getFilePointer();
        long addressesLength = dataDocRangeStart - dataAddressesStart;
        meta.writeLong(addressesLength);

        meta.writeLong(dataDocRangeStart);
        blockDocRangeAcc.build(meta, data);
        long docRangesLen = data.getFilePointer() - dataDocRangeStart;
        meta.writeLong(docRangesLen);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeWhileHandlingException(blockAddressAcc, blockDocRangeAcc);
    }

    /**
     * Like OffsetsAccumulator builds offsets and stores in a DirectMonotonicWriter. But write to temp file
     * rather than directly to a DirectMonotonicWriter because the number of values is unknown.
     */
    static final class DelayedOffsetAccumulator implements Closeable {

        private final Directory dir;
        private final long startOffset;
        private final String metaCodec;
        private final int versionCurrent;
        private final int directMonotonicBlockShift;

        private int numValues = 0;
        private final IndexOutput tempOutput;
        private final String suffix;

        DelayedOffsetAccumulator(
            Directory dir,
            IOContext context,
            IndexOutput data,
            String suffix,
            long startOffset,
            String metaCodec,
            int versionCurrent,
            int directMonotonicBlockShift
        ) throws IOException {
            this.dir = dir;
            this.startOffset = startOffset;
            this.suffix = suffix;
            this.metaCodec = metaCodec;
            this.versionCurrent = versionCurrent;
            this.directMonotonicBlockShift = directMonotonicBlockShift;

            boolean success = false;
            try {
                tempOutput = dir.createTempOutput(data.getName(), suffix, context);
                CodecUtil.writeHeader(tempOutput, metaCodec + suffix, versionCurrent);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(this); // self-close because constructor caller can't
                }
            }
        }

        void addDoc(long delta) throws IOException {
            tempOutput.writeVLong(delta);
            numValues++;
        }

        void build(IndexOutput meta, IndexOutput data) throws IOException {
            CodecUtil.writeFooter(tempOutput);
            IOUtils.close(tempOutput);

            // write the offsets info to the meta file by reading from temp file
            try (ChecksumIndexInput tempInput = dir.openChecksumInput(tempOutput.getName());) {
                CodecUtil.checkHeader(tempInput, metaCodec + suffix, versionCurrent, versionCurrent);
                Throwable priorE = null;
                try {
                    final DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(
                        meta,
                        data,
                        numValues + 1,
                        directMonotonicBlockShift
                    );

                    long offset = startOffset;
                    writer.add(offset);
                    for (int i = 0; i < numValues; ++i) {
                        offset += tempInput.readVLong();
                        writer.add(offset);
                    }
                    writer.finish();
                } catch (Throwable e) {
                    priorE = e;
                } finally {
                    CodecUtil.checkFooter(tempInput, priorE);
                }
            }
        }

        @Override
        public void close() throws IOException {
            if (tempOutput != null) {
                IOUtils.close(tempOutput, () -> dir.deleteFile(tempOutput.getName()));
            }
        }
    }
}
