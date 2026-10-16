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

final class PlainSkipFixture implements Closeable {

    final BytesRef[][] docSlots;
    final int valuesPerBlock;
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
        int valuesPerBlock,
        StringColumnMetadata.Plain.Skipper skipper,
        IndexInput input,
        long leadingBytes
    ) {
        this.docSlots = docSlots;
        this.valuesPerBlock = valuesPerBlock;
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
    static PlainSkipFixture build(Directory dir, BytesRef[][] docSlots, int valuesPerBlock) throws IOException {
        return build(dir, docSlots, valuesPerBlock, false);
    }

    /** As above; with {@code reuseBuffer} every value is handed over in one shared array that is overwritten right after. */
    static PlainSkipFixture build(Directory dir, BytesRef[][] docSlots, int valuesPerBlock, boolean reuseBuffer) throws IOException {
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
            try (PlainSkipBounds.Writer writer = new PlainSkipBounds.Writer(dir, IOContext.DEFAULT, "fixture", valuesPerBlock, total)) {
                long address = 0;
                for (int doc = 0; doc < docSlots.length; doc++) {
                    if (docSlots[doc] == null) {
                        continue;
                    }
                    for (BytesRef value : docSlots[doc]) {
                        if (value == null) {
                            writer.add(address++, doc, null);
                        } else if (reuseBuffer) {
                            shared.bytes = ArrayUtil.grow(shared.bytes, value.length + 8);
                            System.arraycopy(value.bytes, value.offset, shared.bytes, 4, value.length);
                            shared.offset = 4;
                            shared.length = value.length;
                            writer.add(address++, doc, shared);
                            Arrays.fill(shared.bytes, (byte) 0xAA);
                        } else {
                            writer.add(address++, doc, PlainSkipTestValues.maybeOffset(value));
                        }
                    }
                }
                skipper = writer.finish(out, true);
            }
        }
        return new PlainSkipFixture(docSlots, valuesPerBlock, skipper, dir.openInput("fixture.cns", IOContext.DEFAULT), leading);
    }

    /** What the region holds for one block, read straight from the bytes: a null bound is a block of nulls. */
    record Parsed(int firstDoc, BytesRef min, BytesRef max) {}

    /**
     * Reads the region the way the format documents it, without going through the reader: a directory of entries
     * of an int and a long, then a payload of a flag byte and, for a block with a value, two ints and the bytes.
     */
    static List<Parsed> parse(IndexInput file, StringColumnMetadata.Plain.Skipper skipper) throws IOException {
        final long directoryBytes = (long) skipper.blockCount() * 12;
        final byte[] region = new byte[(int) (directoryBytes + skipper.payloadLength())];
        file.seek(skipper.dataOffset());
        file.readBytes(region, 0, region.length);
        final ByteArrayDataInput in = new ByteArrayDataInput(region);
        final List<Parsed> parsed = new ArrayList<>();
        for (int block = 0; block < skipper.blockCount(); block++) {
            in.setPosition(block * 12);
            final int firstDoc = in.readInt();
            final long offset = in.readLong();
            in.setPosition((int) (directoryBytes + offset));
            if (in.readByte() == 0) {
                parsed.add(new Parsed(firstDoc, null, null));
                continue;
            }
            final int minLength = in.readInt();
            final int maxLength = in.readInt();
            final byte[] min = new byte[minLength];
            in.readBytes(min, 0, minLength);
            final byte[] max = new byte[maxLength];
            in.readBytes(max, 0, maxLength);
            parsed.add(new Parsed(firstDoc, new BytesRef(min), new BytesRef(max)));
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
        return new PlainSkipFixture(docSlots, valuesPerBlock, skipper, other, leadingBytes);
    }

    static int randomValuesPerBlock() {
        return 1 << ESTestCase.between(0, 7);
    }

    int blockCount() {
        return PlainSkipBounds.blockCount(numValues, valuesPerBlock);
    }

    int blockShift() {
        return Integer.numberOfTrailingZeros(valuesPerBlock);
    }

    PlainSkipBounds.Reader reader() throws IOException {
        return new PlainSkipBounds.Reader(skipper, input);
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
        };
    }

    DocsPresence presence() {
        return new DocsPresence(docs);
    }

    PlainRangePresence presence(PlainSkipBounds.Query query) throws IOException {
        return new PlainRangePresence(presence(), reader(), query, slots(), blockShift());
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
