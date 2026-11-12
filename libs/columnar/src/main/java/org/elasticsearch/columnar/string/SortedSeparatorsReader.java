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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Reads the region of {@link SortedSeparatorsSkip}. The entry {@code e} holds the values from its separator to the
 * separator of the entry after it, in the other order when the values descend. As the separators are in order, the
 * entries a query rules out because they lie below it are a prefix of the entries and those above it a suffix, so the
 * first entry the query can touch is found by a binary search.
 */
final class SortedSeparatorsReader implements PlainSkipKind.Reader {

    private final RandomAccessInput region;
    private final int entryCount;
    private final long numValues;
    private final long payloadStart;
    private final long payloadLength;
    private final boolean descending;
    private final long endOffset;

    SortedSeparatorsReader(StringColumnMetadata.Plain.Skipper skipper, long numValues, IndexInput skipIndex) throws IOException {
        this.entryCount = skipper.entryCount();
        this.numValues = numValues;
        this.payloadLength = skipper.payloadLength();
        final long directoryBytes = (long) entryCount * SortedSeparatorsSkip.ENTRY_BYTES;
        this.payloadStart = SortedSeparatorsSkip.HEADER_BYTES + directoryBytes;
        final long room = skipIndex.length() - payloadStart;
        if (skipper.dataOffset() < 0 || payloadLength < 0 || payloadLength > room || skipper.dataOffset() > room - payloadLength) {
            throw new CorruptIndexException(
                "plain skip separators at ["
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
        this.region = skipIndex.randomAccessSlice(skipper.dataOffset(), payloadStart + payloadLength);
        final byte direction = region.readByte(0);
        if (direction != SortedSeparatorsSkip.ASCENDING && direction != SortedSeparatorsSkip.DESCENDING) {
            throw new CorruptIndexException("plain skip separators have the direction [" + direction + "]", skipIndex);
        }
        this.descending = direction == SortedSeparatorsSkip.DESCENDING;
        this.endOffset = region.readLong(Byte.BYTES);
    }

    @Override
    public int entryCount() {
        return entryCount;
    }

    @Override
    public int firstDoc(int entry) throws IOException {
        checkEntry(entry);
        final int doc = region.readInt(entryPosition(entry) + SortedSeparatorsSkip.DOC_FIELD);
        if (doc < 0) {
            throw corrupt(entry, "the entry starts at the document [" + doc + "]");
        }
        return doc;
    }

    @Override
    public long firstSlot(int entry) throws IOException {
        checkEntry(entry);
        final long slot = region.readLong(entryPosition(entry));
        if (slot < 0 || slot >= numValues || (entry == 0 && slot != 0)) {
            throw corrupt(entry, "the first slot is [" + slot + "] of [" + numValues + "]");
        }
        return slot;
    }

    @Override
    public long endSlot(int entry) throws IOException {
        final long first = firstSlot(entry);
        final long end = entry + 1 < entryCount ? firstSlot(entry + 1) : numValues;
        if (end <= first) {
            throw corrupt(entry, "the entry ends at the slot [" + end + "] after starting at [" + first + "]");
        }
        return end;
    }

    @Override
    public int entryOf(long slot) throws IOException {
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

    @Override
    public boolean excludes(int entry, PlainSkipQuery query) throws IOException {
        checkEntry(entry);
        if (tooLow(entry, query) || tooHigh(entry, query)) {
            requireOrdered(entry);
            return true;
        }
        return false;
    }

    @Override
    public int nextCandidate(int fromEntry, PlainSkipQuery query) throws IOException {
        int low = Math.max(fromEntry, 0);
        int high = entryCount;
        while (low < high) {
            final int middle = (low + high) >>> 1;
            if (belowQuery(middle, query)) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        if (low == entryCount || aboveQuery(low, query)) {
            return entryCount;
        }
        assert excludes(low, query) == false : "the first entry the query can touch is not ruled out";
        return low;
    }

    private boolean belowQuery(int entry, PlainSkipQuery query) throws IOException {
        return descending ? tooHigh(entry, query) : tooLow(entry, query);
    }

    @Override
    public int candidatesEnd(int fromEntry, PlainSkipQuery query) throws IOException {
        int low = fromEntry;
        int high = entryCount;
        while (low < high) {
            final int middle = (low + high) >>> 1;
            if (aboveQuery(middle, query)) {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        return low;
    }

    private boolean aboveQuery(int entry, PlainSkipQuery query) throws IOException {
        return descending ? tooLow(entry, query) : tooHigh(entry, query);
    }

    private boolean tooLow(int entry, PlainSkipQuery query) throws IOException {
        final int max = descending ? entry : entry + 1;
        if (query.prefix() != null) {
            return compare(max, query.prefix()) < 0;
        }
        if (query.lower() == null) {
            return false;
        }
        final int comparison = compare(max, query.lower());
        return comparison < 0 || (comparison == 0 && query.includeLower() == false);
    }

    private boolean tooHigh(int entry, PlainSkipQuery query) throws IOException {
        final int min = descending ? entry + 1 : entry;
        if (query.prefix() != null) {
            return compare(min, query.prefix()) > 0 && startsWith(min, query.prefix()) == false;
        }
        if (query.upper() == null) {
            return false;
        }
        final int comparison = compare(min, query.upper());
        return comparison > 0 || (comparison == 0 && query.includeUpper() == false);
    }

    private void requireOrdered(int entry) throws IOException {
        final int comparison = compareSeparators(entry, entry + 1);
        if (descending ? comparison < 0 : comparison > 0) {
            throw corrupt(entry, "the separators are not in order");
        }
    }

    private void checkEntry(int entry) throws IOException {
        if (entry < 0 || entry >= entryCount) {
            throw new CorruptIndexException("plain skip entry [" + entry + "] is outside [0, " + entryCount + ")", region.toString());
        }
    }

    private long entryPosition(int entry) {
        return SortedSeparatorsSkip.HEADER_BYTES + (long) entry * SortedSeparatorsSkip.ENTRY_BYTES;
    }

    /** Where the separator {@code index} starts in the region; the index {@code entryCount} is the last value. */
    private long separatorPosition(int index) throws IOException {
        final long offset = index == entryCount ? endOffset : region.readLong(entryPosition(index) + SortedSeparatorsSkip.OFFSET_FIELD);
        if (offset < 0 || offset + SortedSeparatorsSkip.LENGTH_BYTES > payloadLength) {
            throw corrupt(index, "payload offset [" + offset + "] is outside [0, " + payloadLength + ")");
        }
        return payloadStart + offset;
    }

    private int separatorLength(long position, int index) throws IOException {
        final int length = region.readInt(position);
        if (length < 0 || position + SortedSeparatorsSkip.LENGTH_BYTES + length > payloadStart + payloadLength) {
            throw corrupt(index, "a separator of [" + length + "] bytes at [" + position + "]");
        }
        return length;
    }

    private int compare(int index, BytesRef other) throws IOException {
        final long position = separatorPosition(index);
        final int length = separatorLength(position, index);
        final long bytes = position + SortedSeparatorsSkip.LENGTH_BYTES;
        final int common = Math.min(length, other.length);
        for (int i = 0; i < common; i++) {
            final int diff = (region.readByte(bytes + i) & 0xFF) - (other.bytes[other.offset + i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return length - other.length;
    }

    private boolean startsWith(int index, BytesRef prefix) throws IOException {
        final long position = separatorPosition(index);
        if (separatorLength(position, index) < prefix.length) {
            return false;
        }
        final long bytes = position + SortedSeparatorsSkip.LENGTH_BYTES;
        for (int i = 0; i < prefix.length; i++) {
            if (region.readByte(bytes + i) != prefix.bytes[prefix.offset + i]) {
                return false;
            }
        }
        return true;
    }

    private int compareSeparators(int left, int right) throws IOException {
        final long leftPosition = separatorPosition(left);
        final long rightPosition = separatorPosition(right);
        final int leftLength = separatorLength(leftPosition, left);
        final int rightLength = separatorLength(rightPosition, right);
        final long leftBytes = leftPosition + SortedSeparatorsSkip.LENGTH_BYTES;
        final long rightBytes = rightPosition + SortedSeparatorsSkip.LENGTH_BYTES;
        final int common = Math.min(leftLength, rightLength);
        for (int i = 0; i < common; i++) {
            final int diff = (region.readByte(leftBytes + i) & 0xFF) - (region.readByte(rightBytes + i) & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return leftLength - rightLength;
    }

    private CorruptIndexException corrupt(int entry, String reason) {
        return new CorruptIndexException("invalid plain skip entry [" + entry + "]: " + reason, region.toString());
    }
}
