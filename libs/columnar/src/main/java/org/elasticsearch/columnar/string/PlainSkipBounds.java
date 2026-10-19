/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;

/**
 * The exact smallest and largest value of every block of {@code valuesPerBlock} slots of a plain column, in the
 * segment's skip-index file. A block is the unit a read loads as one span. A block ruled out is not loaded for a
 * document whose slots all lie in ruled-out blocks; a kept document still compares every slot it holds.
 *
 * <p>The region is a directory of fixed size entries, one per block (the document holding its first slot and the
 * offset of its bounds), followed by the bounds: a flag and, for a block with a value, the two lengths and the bytes.
 * A reader reads only the entries it needs and compares the bytes where they lie.
 */
final class PlainSkipBounds {

    private static final byte EMPTY = 0;

    private static final byte BOUNDED = 1;

    private static final int ENTRY_BYTES = Integer.BYTES + Long.BYTES;

    private static final int BOUNDED_HEADER_BYTES = Byte.BYTES + 2 * Integer.BYTES;

    private PlainSkipBounds() {}

    static int blockCount(long numValues, int valuesPerBlock) {
        return Math.toIntExact(numValues / valuesPerBlock + (numValues % valuesPerBlock == 0 ? 0 : 1));
    }

    record Query(BytesRef lower, boolean includeLower, BytesRef upper, boolean includeUpper, BytesRef prefix) {

        static Query range(BytesRef lower, boolean includeLower, BytesRef upper, boolean includeUpper) {
            return new Query(lower, includeLower, upper, includeUpper, null);
        }

        static Query prefix(BytesRef prefix) {
            return new Query(null, false, null, false, prefix);
        }
    }

    static final class Writer implements Closeable {

        private final Directory directory;
        private final IndexOutput directoryOutput;
        private final IndexOutput payloadOutput;
        private final int blockShift;
        private final int blockCount;
        private final BytesRefBuilder min = new BytesRefBuilder();
        private final BytesRefBuilder max = new BytesRefBuilder();
        private int block = -1;
        private int firstDoc;
        private boolean hasValue;
        private int blocksWritten;
        private boolean outputsClosed;

        Writer(Directory directory, IOContext context, String dataName, int valuesPerBlock, long numValues) throws IOException {
            this.directory = directory;
            this.blockShift = Integer.numberOfTrailingZeros(valuesPerBlock);
            this.blockCount = blockCount(numValues, valuesPerBlock);
            this.directoryOutput = directory.createTempOutput(dataName, "columnar-plain-bounds-directory", context);
            IndexOutput payload = null;
            try {
                payload = directory.createTempOutput(dataName, "columnar-plain-bounds-payload", context);
            } finally {
                if (payload == null) {
                    IOUtils.closeWhileHandlingException(directoryOutput);
                    IOUtils.deleteFilesIgnoringExceptions(directory, directoryOutput.getName());
                }
            }
            this.payloadOutput = payload;
        }

        /** Takes the slot at {@code address}, held by {@code doc}; a null {@code value} is a null slot. */
        void add(long address, int doc, BytesRef value) throws IOException {
            final long slotBlock = address >>> blockShift;
            if (slotBlock != block) {
                if (slotBlock != block + 1L) {
                    throw new IllegalStateException("slot [" + address + "] arrived after the block [" + block + "]");
                }
                writeCurrent();
                block = (int) slotBlock;
                firstDoc = doc;
                hasValue = false;
            }
            if (value == null) {
                return;
            }
            if (hasValue == false) {
                min.copyBytes(value);
                max.copyBytes(value);
                hasValue = true;
            } else if (value.compareTo(min.get()) < 0) {
                min.copyBytes(value);
            } else if (value.compareTo(max.get()) > 0) {
                max.copyBytes(value);
            }
        }

        /** Appends the region to {@code skipIndex} and returns where it went, or null if not {@code keep} or there are no slots. */
        StringColumnMetadata.Plain.Skipper finish(IndexOutput skipIndex, boolean keep) throws IOException {
            writeCurrent();
            if (blocksWritten != blockCount) {
                throw new IllegalStateException("wrote [" + blocksWritten + "] blocks for [" + blockCount + "] expected");
            }
            final long payloadLength = payloadOutput.getFilePointer();
            closeOutputs();
            if (keep == false || blockCount == 0) {
                close();
                return null;
            }
            final long offset = skipIndex.getFilePointer();
            try {
                append(directoryOutput.getName(), skipIndex);
                append(payloadOutput.getName(), skipIndex);
            } finally {
                close();
            }
            return new StringColumnMetadata.Plain.Skipper(offset, blockCount, payloadLength);
        }

        @Override
        public void close() throws IOException {
            try {
                closeOutputs();
            } finally {
                IOUtils.deleteFilesIgnoringExceptions(directory, directoryOutput.getName(), payloadOutput.getName());
            }
        }

        private void append(String staged, IndexOutput skipIndex) throws IOException {
            try (IndexInput in = directory.openInput(staged, IOContext.READONCE)) {
                skipIndex.copyBytes(in, in.length());
            }
        }

        private void closeOutputs() throws IOException {
            if (outputsClosed == false) {
                outputsClosed = true;
                IOUtils.close(directoryOutput, payloadOutput);
            }
        }

        private void writeCurrent() throws IOException {
            if (block < 0) {
                return;
            }
            directoryOutput.writeInt(firstDoc);
            directoryOutput.writeLong(payloadOutput.getFilePointer());
            if (hasValue) {
                payloadOutput.writeByte(BOUNDED);
                payloadOutput.writeInt(min.length());
                payloadOutput.writeInt(max.length());
                payloadOutput.writeBytes(min.bytes(), 0, min.length());
                payloadOutput.writeBytes(max.bytes(), 0, max.length());
            } else {
                payloadOutput.writeByte(EMPTY);
            }
            blocksWritten++;
            block = -1;
            hasValue = false;
        }
    }

    static final class Reader {

        private final RandomAccessInput region;
        private final int blockCount;
        private final long directoryBytes;
        private final long payloadLength;
        private boolean bounded;
        private long minPosition;
        private int minLength;
        private long maxPosition;
        private int maxLength;

        Reader(StringColumnMetadata.Plain.Skipper skipper, IndexInput skipIndex) throws IOException {
            this.blockCount = skipper.blockCount();
            this.payloadLength = skipper.payloadLength();
            this.directoryBytes = (long) blockCount * ENTRY_BYTES;
            final long room = skipIndex.length() - directoryBytes;
            if (skipper.dataOffset() < 0 || payloadLength < 0 || payloadLength > room || skipper.dataOffset() > room - payloadLength) {
                throw new CorruptIndexException(
                    "plain skip bounds at ["
                        + skipper.dataOffset()
                        + "] with ["
                        + blockCount
                        + "] blocks and ["
                        + payloadLength
                        + "] payload bytes are outside the skip index of ["
                        + skipIndex.length()
                        + "] bytes",
                    skipIndex
                );
            }
            final long length = directoryBytes + payloadLength;
            this.region = skipIndex.randomAccessSlice(skipper.dataOffset(), length);
        }

        int blockCount() {
            return blockCount;
        }

        int firstDoc(int block) throws IOException {
            checkBlock(block);
            final int doc = region.readInt((long) block * ENTRY_BYTES);
            if (doc < 0) {
                throw new CorruptIndexException("plain skip block [" + block + "] starts at the document [" + doc + "]", region.toString());
            }
            return doc;
        }

        /** Whether the bounds of {@code block} prove that no slot in it can match {@code query}. */
        boolean excludes(int block, Query query) throws IOException {
            load(block);
            if (bounded == false) {
                return true;
            }
            if (rulesOut(query)) {
                requireOrdered(block);
                return true;
            }
            return false;
        }

        private boolean rulesOut(Query query) throws IOException {
            if (query.prefix() != null) {
                return compareMax(query.prefix()) < 0 || (compareMin(query.prefix()) > 0 && minStartsWith(query.prefix()) == false);
            }
            if (query.upper() != null) {
                final int cmp = compareMin(query.upper());
                if (cmp > 0 || (cmp == 0 && query.includeUpper() == false)) {
                    return true;
                }
            }
            if (query.lower() != null) {
                final int cmp = compareMax(query.lower());
                return cmp < 0 || (cmp == 0 && query.includeLower() == false);
            }
            return false;
        }

        private void checkBlock(int block) throws IOException {
            if (block < 0 || block >= blockCount) {
                throw new CorruptIndexException("plain skip block [" + block + "] is outside [0, " + blockCount + ")", region.toString());
            }
        }

        private void load(int block) throws IOException {
            checkBlock(block);
            final long offset = region.readLong((long) block * ENTRY_BYTES + Integer.BYTES);
            if (offset < 0 || offset >= payloadLength) {
                throw corrupt(block, "payload offset [" + offset + "] is outside [0, " + payloadLength + ")");
            }
            if (block == 0 && offset != 0) {
                throw corrupt(block, "the first payload offset is [" + offset + "]");
            }
            final long end = block + 1 < blockCount ? region.readLong((long) (block + 1) * ENTRY_BYTES + Integer.BYTES) : payloadLength;
            final long at = directoryBytes + offset;
            final byte flag = region.readByte(at);
            if (flag == EMPTY) {
                if (offset + Byte.BYTES != end) {
                    throw corrupt(block, "an empty entry at payload offset [" + offset + "] ends at [" + end + "]");
                }
                bounded = false;
                return;
            }
            if (flag != BOUNDED || offset + BOUNDED_HEADER_BYTES > payloadLength) {
                throw corrupt(block, "flag [" + flag + "] at payload offset [" + offset + "]");
            }
            minLength = region.readInt(at + Byte.BYTES);
            maxLength = region.readInt(at + Byte.BYTES + Integer.BYTES);
            minPosition = at + BOUNDED_HEADER_BYTES;
            maxPosition = minPosition + minLength;
            if (minLength < 0 || maxLength < 0 || offset + BOUNDED_HEADER_BYTES + (long) minLength + maxLength != end) {
                throw corrupt(block, "bounds of [" + minLength + ", " + maxLength + "] bytes at payload offset [" + offset + "]");
            }
            bounded = true;
        }

        private void requireOrdered(int block) throws IOException {
            final int common = Math.min(minLength, maxLength);
            for (int i = 0; i < common; i++) {
                final int diff = (region.readByte(minPosition + i) & 0xFF) - (region.readByte(maxPosition + i) & 0xFF);
                if (diff != 0) {
                    if (diff > 0) {
                        throw corrupt(block, "the minimum is larger than the maximum");
                    }
                    return;
                }
            }
            if (minLength > maxLength) {
                throw corrupt(block, "the minimum is larger than the maximum");
            }
        }

        private CorruptIndexException corrupt(int block, String reason) {
            return new CorruptIndexException("invalid plain skip block [" + block + "]: " + reason, region.toString());
        }

        private int compareMin(BytesRef other) throws IOException {
            return compare(minPosition, minLength, other);
        }

        private int compareMax(BytesRef other) throws IOException {
            return compare(maxPosition, maxLength, other);
        }

        private boolean minStartsWith(BytesRef prefix) throws IOException {
            if (minLength < prefix.length) {
                return false;
            }
            for (int i = 0; i < prefix.length; i++) {
                if (region.readByte(minPosition + i) != prefix.bytes[prefix.offset + i]) {
                    return false;
                }
            }
            return true;
        }

        private int compare(long position, int length, BytesRef other) throws IOException {
            final int common = Math.min(length, other.length);
            for (int i = 0; i < common; i++) {
                final int diff = (region.readByte(position + i) & 0xFF) - (other.bytes[other.offset + i] & 0xFF);
                if (diff != 0) {
                    return diff;
                }
            }
            return length - other.length;
        }
    }
}
