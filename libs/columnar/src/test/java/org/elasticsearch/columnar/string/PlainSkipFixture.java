/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.test.ESTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

final class PlainSkipFixture implements Closeable {

    final BytesRef[][] docSlots;
    final int[] ownerOfSlot;
    final int[] entryOfSlot;
    final long[] entryFirstSlot;
    final long numValues;
    final StringColumnMetadata.Plain.Skipper skipper;
    final IndexInput input;
    final long leadingBytes;
    final List<Integer> docs = new ArrayList<>();
    final long[] firstAddress;
    final int[] slotCounts;
    final BytesRef[] slotValues;
    final int[] slotDocs;

    private PlainSkipFixture(
        BytesRef[][] docSlots,
        int[] ownerOfSlot,
        StringColumnMetadata.Plain.Skipper skipper,
        IndexInput input,
        long leadingBytes
    ) {
        this.docSlots = docSlots;
        this.ownerOfSlot = ownerOfSlot;
        this.skipper = skipper;
        this.input = input;
        this.leadingBytes = leadingBytes;
        long total = 0;
        for (BytesRef[] slots : docSlots) {
            if (slots != null) {
                total += slots.length;
            }
        }
        this.numValues = total;
        this.entryOfSlot = new int[(int) total];
        final long[] firsts = new long[(int) total];
        int entries = 0;
        for (int slot = 0; slot < total; slot++) {
            if (slot == 0 || ownerOfSlot[slot] != ownerOfSlot[slot - 1]) {
                firsts[entries++] = slot;
            }
            entryOfSlot[slot] = entries - 1;
        }
        this.entryFirstSlot = Arrays.copyOf(firsts, entries);
        this.slotValues = new BytesRef[(int) total];
        this.slotDocs = new int[(int) total];
        for (int doc = 0, address = 0; doc < docSlots.length; doc++) {
            if (docSlots[doc] != null) {
                docs.add(doc);
                for (BytesRef value : docSlots[doc]) {
                    slotValues[address] = value;
                    slotDocs[address++] = doc;
                }
            }
        }
        this.firstAddress = new long[docs.size()];
        this.slotCounts = new int[docs.size()];
        long address = 0;
        for (int rank = 0; rank < docs.size(); rank++) {
            firstAddress[rank] = address;
            slotCounts[rank] = docSlots[docs.get(rank)].length;
            address += slotCounts[rank];
        }
    }

    /**
     * A document with a null entry in {@code docSlots} holds no slot and is not present; one with an empty array is
     * present and holds none. A null element is a null slot.
     */
    static PlainSkipFixture build(Directory dir, BytesRef[][] docSlots, int slotsPerEntry) throws IOException {
        return build(dir, docSlots, slotsPerEntry, false);
    }

    /** As above; with {@code reuseBuffer} every value is handed over in one shared array that is overwritten right after. */
    static PlainSkipFixture build(Directory dir, BytesRef[][] docSlots, int slotsPerEntry, boolean reuseBuffer) throws IOException {
        return build(dir, docSlots, evenOwners(docSlots, slotsPerEntry), reuseBuffer);
    }

    /** Owners that start a new chunk every {@code slotsPerEntry} slots. */
    static int[] evenOwners(BytesRef[][] docSlots, int slotsPerEntry) {
        int total = 0;
        for (BytesRef[] slots : docSlots) {
            total += slots == null ? 0 : slots.length;
        }
        final int[] owners = new int[total];
        for (int slot = 0; slot < total; slot++) {
            owners[slot] = slot / slotsPerEntry;
        }
        return owners;
    }

    /** The owners a real column written with {@code sizes} gets, from the rules of the chunk writer. */
    static int[] realOwners(BytesRef[][] docSlots, StringColumnOptions.Sizes sizes) {
        final List<BytesRef> slots = new ArrayList<>();
        for (BytesRef[] docSlot : docSlots) {
            if (docSlot != null) {
                slots.addAll(Arrays.asList(docSlot));
            }
        }
        return PlainChunkModel.owners(slots, sizes.plainChunks(), sizes.valuesPerBlock(), sizes.lengthBlockSize());
    }

    static int entriesOf(int[] owners) {
        int entries = 0;
        for (int slot = 0; slot < owners.length; slot++) {
            if (slot == 0 || owners[slot] != owners[slot - 1]) {
                entries++;
            }
        }
        return entries;
    }

    /** As above, with the chunk that owns each slot given; it must never go back. */
    static PlainSkipFixture build(Directory dir, BytesRef[][] docSlots, int[] owners, boolean reuseBuffer) throws IOException {
        final BytesRef shared = new BytesRef(new byte[8192]);
        final int leading = ESTestCase.between(0, 9);
        long total = 0;
        for (BytesRef[] slots : docSlots) {
            total += slots == null ? 0 : slots.length;
        }
        final StringColumnMetadata.Plain.Skipper skipper;
        try (IndexOutput out = dir.createOutput("fixture.cns", IOContext.DEFAULT)) {
            for (int i = 0; i < leading; i++) {
                out.writeByte((byte) i);
            }
            try (PlainSkipKind.Writer writer = PlainSkipKind.forWriting().newWriter(dir, IOContext.DEFAULT, "fixture", total)) {
                long address = 0;
                BytesRef previous = null;
                for (int doc = 0; doc < docSlots.length; doc++) {
                    if (docSlots[doc] == null) {
                        continue;
                    }
                    for (BytesRef value : docSlots[doc]) {
                        final boolean repeat = value != null && value.equals(previous);
                        previous = value;
                        if (value == null) {
                            writer.add(address, doc, owners[(int) address++], null, false);
                        } else if (reuseBuffer) {
                            shared.bytes = ArrayUtil.grow(shared.bytes, value.length + 8);
                            System.arraycopy(value.bytes, value.offset, shared.bytes, 4, value.length);
                            shared.offset = 4;
                            shared.length = value.length;
                            writer.add(address, doc, owners[(int) address++], shared, repeat);
                            Arrays.fill(shared.bytes, (byte) 0xAA);
                        } else {
                            writer.add(address, doc, owners[(int) address++], PlainSkipTestValues.maybeOffset(value), repeat);
                        }
                    }
                }
                skipper = writer.finish(out);
            }
        }
        return new PlainSkipFixture(docSlots, owners, skipper, dir.openInput("fixture.cns", IOContext.DEFAULT), leading);
    }

    /** What the region holds for one entry, read straight from the bytes: the lowest and highest value it can hold. */
    record Parsed(long firstSlot, int firstDoc, BytesRef min, BytesRef max) {}

    /** Where the second entry stores its first document in a region: after the header and the first entry. */
    static final int SECOND_ENTRY_FIRST_DOC = SortedSeparatorsSkip.HEADER_BYTES + SortedSeparatorsSkip.ENTRY_BYTES
        + SortedSeparatorsSkip.DOC_FIELD;

    /**
     * Reads the region the way the format documents it, without going through the reader: a header of the direction
     * and the offset of the last value, a directory of entries of a long, an int and a long, then a payload of lengths
     * and bytes.
     */
    static List<Parsed> parse(IndexInput file, StringColumnMetadata.Plain.Skipper skipper) throws IOException {
        final int directoryBytes = skipper.entryCount() * SortedSeparatorsSkip.ENTRY_BYTES;
        final int payloadStart = SortedSeparatorsSkip.HEADER_BYTES + directoryBytes;
        final byte[] region = new byte[(int) (payloadStart + skipper.payloadLength())];
        file.seek(skipper.dataOffset());
        file.readBytes(region, 0, region.length);
        final ByteArrayDataInput in = new ByteArrayDataInput(region);
        final boolean descending = in.readByte() == SortedSeparatorsSkip.DESCENDING;
        final long endOffset = in.readLong();
        final BytesRef[] separators = new BytesRef[skipper.entryCount() + 1];
        final long[] firstSlots = new long[skipper.entryCount()];
        final int[] firstDocs = new int[skipper.entryCount()];
        for (int entry = 0; entry <= skipper.entryCount(); entry++) {
            long offset = endOffset;
            if (entry < skipper.entryCount()) {
                in.setPosition(SortedSeparatorsSkip.HEADER_BYTES + entry * SortedSeparatorsSkip.ENTRY_BYTES);
                firstSlots[entry] = in.readLong();
                firstDocs[entry] = in.readInt();
                offset = in.readLong();
            }
            in.setPosition((int) (payloadStart + offset));
            final byte[] bytes = new byte[in.readInt()];
            in.readBytes(bytes, 0, bytes.length);
            separators[entry] = new BytesRef(bytes);
        }
        final List<Parsed> parsed = new ArrayList<>();
        for (int entry = 0; entry < skipper.entryCount(); entry++) {
            final BytesRef own = separators[entry];
            final BytesRef next = separators[entry + 1];
            parsed.add(new Parsed(firstSlots[entry], firstDocs[entry], descending ? next : own, descending ? own : next));
        }
        return parsed;
    }

    /** The same column over a copy of the skip-index file with one byte of the region replaced. */
    PlainSkipFixture withByte(Directory dir, long regionPosition, byte replacement) throws IOException {
        final byte[] file = new byte[(int) input.length()];
        input.seek(0);
        input.readBytes(file, 0, file.length);
        file[(int) (leadingBytes + regionPosition)] = replacement;
        final String name = "corrupt-" + regionPosition + "-" + ESTestCase.randomAlphaOfLength(4) + ".cns";
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            out.writeBytes(file, file.length);
        }
        return over(dir.openInput(name, IOContext.DEFAULT));
    }

    PlainSkipFixture over(IndexInput other) {
        return new PlainSkipFixture(docSlots, ownerOfSlot, skipper, other, leadingBytes);
    }

    static int randomValuesPerBlock() {
        return 1 << ESTestCase.between(0, 7);
    }

    int entryCount() {
        return entryFirstSlot.length;
    }

    PlainSkipKind.Reader reader() throws IOException {
        return PlainSkipKind.forId(skipper.kind()).newReader(skipper, numValues, input);
    }

    PlainRangePresence.Slots slots() {
        return new PlainRangePresence.Slots() {
            @Override
            public long firstAddress(int rank) {
                return firstAddress[rank];
            }

            @Override
            public long count(int rank) {
                return slotCounts[rank];
            }

            @Override
            public boolean oneSlotADocument() {
                return Arrays.stream(slotCounts).allMatch(count -> count == 1);
            }

            @Override
            public boolean everySlotHoldsAValue() {
                return Arrays.stream(slotValues).allMatch(Objects::nonNull);
            }
        };
    }

    DocsPresence presence() {
        return new DocsPresence(docs);
    }

    PlainRangePresence presence(PlainSkipQuery query) throws IOException {
        return new PlainRangePresence(presence(), reader(), query, slots());
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    static final class DocsPresence extends ColumnIterator {
        final List<Integer> docs;
        private int at = -1;

        DocsPresence(List<Integer> docs) {
            this.docs = docs;
        }

        @Override
        public int rank() {
            return at;
        }

        @Override
        public boolean advanceExact(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docID() {
            return at < 0 ? -1 : at >= docs.size() ? NO_MORE_DOCS : docs.get(at);
        }

        @Override
        public int nextDoc() {
            at = Math.min(at + 1, docs.size());
            return docID();
        }

        @Override
        public int advance(int target) {
            while (at < docs.size() && (at < 0 || docs.get(at) < target)) {
                at++;
            }
            return docID();
        }

        @Override
        public int docIDRunEnd() {
            int end = at;
            while (end + 1 < docs.size() && docs.get(end + 1) == docs.get(end) + 1) {
                end++;
            }
            return docs.get(end) + 1;
        }

        @Override
        public long cost() {
            return docs.size();
        }
    }
}
