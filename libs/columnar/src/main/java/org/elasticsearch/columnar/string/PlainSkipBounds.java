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
 * The exact smallest and largest value of the slots each compressed chunk of a plain column owns, in the segment's
 * skip-index file. A chunk is the unit that is decompressed. A slot is owned by the chunk holding its first byte, or
 * by the chunk being filled when it has no bytes (a null, an empty value or a repeat), so the slots of an entry are
 * contiguous. A value that crosses into the next chunk is bounded by the entry that owns it only. An entry ruled
 * out is not read for a document whose slots are all owned by ruled-out entries; a kept document still compares
 * every slot it holds.
 *
 * <p>The region is a directory of fixed size entries (the first slot an entry owns, the document holding it and
 * the offset of its bounds), followed by the bounds: a flag and, for an entry with a value, the two lengths and the
 * bytes. A reader reads only the entries it needs and compares the bytes where they lie.
 */
final class PlainSkipBounds {

    private static final byte EMPTY = 0;

    private static final byte BOUNDED = 1;

    private static final int ENTRY_BYTES = Long.BYTES + Integer.BYTES + Long.BYTES;

    private static final int DOC_FIELD = Long.BYTES;

    private static final int OFFSET_FIELD = Long.BYTES + Integer.BYTES;

    private static final int BOUNDED_HEADER_BYTES = Byte.BYTES + 2 * Integer.BYTES;

    private PlainSkipBounds() {}

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
        private final long numValues;
        private final BytesRefBuilder min = new BytesRefBuilder();
        private final BytesRefBuilder max = new BytesRefBuilder();
        private int ownerChunk = -1;
        private long firstSlot;
        private int firstDoc;
        private boolean hasValue;
        private long nextAddress;
        private int entriesWritten;
        private boolean outputsClosed;

        Writer(Directory directory, IOContext context, String dataName, long numValues) throws IOException {
            this.directory = directory;
            this.numValues = numValues;
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

        /**
         * Takes the slot at {@code address}, held by {@code doc} and owned by {@code chunk}; a null {@code value} is a
         * null slot.
         */
        void add(long address, int doc, int chunk, BytesRef value) throws IOException {
            if (address != nextAddress) {
                throw new IllegalStateException("slot [" + address + "] arrived when [" + nextAddress + "] was expected");
            }
            if (chunk < ownerChunk) {
                throw new IllegalStateException("slot [" + address + "] is owned by the chunk [" + chunk + "] after [" + ownerChunk + "]");
            }
            nextAddress++;
            if (chunk != ownerChunk) {
                writeCurrent();
                ownerChunk = chunk;
                firstSlot = address;
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

        /** Appends the region to {@code skipIndex} and returns where it went, or null if there are no slots. */
        StringColumnMetadata.Plain.Skipper finish(IndexOutput skipIndex) throws IOException {
            writeCurrent();
            if (nextAddress != numValues) {
                throw new IllegalStateException("took [" + nextAddress + "] slots for [" + numValues + "] expected");
            }
            final long payloadLength = payloadOutput.getFilePointer();
            closeOutputs();
            if (entriesWritten == 0) {
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
            return new StringColumnMetadata.Plain.Skipper(offset, entriesWritten, payloadLength);
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
            if (ownerChunk < 0) {
                return;
            }
            directoryOutput.writeLong(firstSlot);
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
            entriesWritten++;
            hasValue = false;
        }
    }

    static final class Reader {

        private final RandomAccessInput region;
        private final int entryCount;
        private final long numValues;
        private final long directoryBytes;
        private final long payloadLength;
        private boolean bounded;
        private long minPosition;
        private int minLength;
        private long maxPosition;
        private int maxLength;

        Reader(StringColumnMetadata.Plain.Skipper skipper, long numValues, IndexInput skipIndex) throws IOException {
            this.entryCount = skipper.entryCount();
            this.numValues = numValues;
            this.payloadLength = skipper.payloadLength();
            this.directoryBytes = (long) entryCount * ENTRY_BYTES;
            final long room = skipIndex.length() - directoryBytes;
            if (skipper.dataOffset() < 0 || payloadLength < 0 || payloadLength > room || skipper.dataOffset() > room - payloadLength) {
                throw new CorruptIndexException(
                    "plain skip bounds at ["
                        + skipper.dataOffset()
                        + "] with ["
                        + entryCount
                        + "] entries and ["
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

        int entryCount() {
            return entryCount;
        }

        int firstDoc(int entry) throws IOException {
            checkEntry(entry);
            final int doc = region.readInt((long) entry * ENTRY_BYTES + DOC_FIELD);
            if (doc < 0) {
                throw new CorruptIndexException("plain skip entry [" + entry + "] starts at the document [" + doc + "]", region.toString());
            }
            return doc;
        }

        long firstSlot(int entry) throws IOException {
            checkEntry(entry);
            final long slot = region.readLong((long) entry * ENTRY_BYTES);
            if (slot < 0 || slot >= numValues || (entry == 0 && slot != 0)) {
                throw corrupt(entry, "the first slot is [" + slot + "] of [" + numValues + "]");
            }
            return slot;
        }

        /** The slot after the last one {@code entry} owns. */
        long endSlot(int entry) throws IOException {
            final long first = firstSlot(entry);
            final long end = entry + 1 < entryCount ? firstSlot(entry + 1) : numValues;
            if (end <= first) {
                throw corrupt(entry, "the entry ends at the slot [" + end + "] after starting at [" + first + "]");
            }
            return end;
        }

        /** The entry that owns {@code slot}. */
        int entryOf(long slot) throws IOException {
            int low = 0;
            int high = entryCount - 1;
            while (low < high) {
                final int middle = (low + high + 1) >>> 1;
                if (firstSlot(middle) <= slot) {
                    low = middle;
                } else {
                    high = middle - 1;
                }
            }
            if (slot < firstSlot(low) || slot >= endSlot(low)) {
                throw corrupt(low, "the slot [" + slot + "] is outside the entry");
            }
            return low;
        }

        /** Whether the bounds of {@code entry} prove that no slot it owns can match {@code query}. */
        boolean excludes(int entry, Query query) throws IOException {
            load(entry);
            if (bounded == false) {
                return true;
            }
            if (rulesOut(query)) {
                requireOrdered(entry);
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

        private void checkEntry(int entry) throws IOException {
            if (entry < 0 || entry >= entryCount) {
                throw new CorruptIndexException("plain skip entry [" + entry + "] is outside [0, " + entryCount + ")", region.toString());
            }
        }

        private void load(int entry) throws IOException {
            checkEntry(entry);
            final long offset = region.readLong((long) entry * ENTRY_BYTES + OFFSET_FIELD);
            if (offset < 0 || offset >= payloadLength) {
                throw corrupt(entry, "payload offset [" + offset + "] is outside [0, " + payloadLength + ")");
            }
            if (entry == 0 && offset != 0) {
                throw corrupt(entry, "the first payload offset is [" + offset + "]");
            }
            final long end = entry + 1 < entryCount ? region.readLong((long) (entry + 1) * ENTRY_BYTES + OFFSET_FIELD) : payloadLength;
            final long at = directoryBytes + offset;
            final byte flag = region.readByte(at);
            if (flag == EMPTY) {
                if (offset + Byte.BYTES != end) {
                    throw corrupt(entry, "an empty entry at payload offset [" + offset + "] ends at [" + end + "]");
                }
                bounded = false;
                return;
            }
            if (flag != BOUNDED || offset + BOUNDED_HEADER_BYTES > payloadLength) {
                throw corrupt(entry, "flag [" + flag + "] at payload offset [" + offset + "]");
            }
            minLength = region.readInt(at + Byte.BYTES);
            maxLength = region.readInt(at + Byte.BYTES + Integer.BYTES);
            minPosition = at + BOUNDED_HEADER_BYTES;
            maxPosition = minPosition + minLength;
            if (minLength < 0 || maxLength < 0 || offset + BOUNDED_HEADER_BYTES + (long) minLength + maxLength != end) {
                throw corrupt(entry, "bounds of [" + minLength + ", " + maxLength + "] bytes at payload offset [" + offset + "]");
            }
            bounded = true;
        }

        private void requireOrdered(int entry) throws IOException {
            final int common = Math.min(minLength, maxLength);
            for (int i = 0; i < common; i++) {
                final int diff = (region.readByte(minPosition + i) & 0xFF) - (region.readByte(maxPosition + i) & 0xFF);
                if (diff != 0) {
                    if (diff > 0) {
                        throw corrupt(entry, "the minimum is larger than the maximum");
                    }
                    return;
                }
            }
            if (minLength > maxLength) {
                throw corrupt(entry, "the minimum is larger than the maximum");
            }
        }

        private CorruptIndexException corrupt(int entry, String reason) {
            return new CorruptIndexException("invalid plain skip entry [" + entry + "]: " + reason, region.toString());
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
