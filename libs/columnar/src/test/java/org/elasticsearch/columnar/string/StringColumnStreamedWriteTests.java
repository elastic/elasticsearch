/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.BytesSink;
import org.elasticsearch.columnar.substrate.ChunkCodec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * A plain column written from a cursor that streams its values, as a merge of plain columns is: every value handed over
 * a piece at a time and never asked for whole, and the cursor saying which values repeat. Values run from empty to many
 * chunks long, so blocks of them hold more bytes than a chunk and are read back a value at a time.
 */
public class StringColumnStreamedWriteTests extends ColumnarStringTestCase {

    public void testStreamedValuesReadBack() throws IOException {
        for (int iter = 0; iter < 10; iter++) {
            final StringColumnOptions options = StringColumnOptions.PAYLOADS;
            final StringColumnOptions withCodec = new StringColumnOptions(
                options.dictionary(),
                options.summary(),
                randomFrom(ChunkCodec.values()),
                options.sizes(),
                options.singleValued(),
                options.enableSkipIndex()
            );
            final int chunk = withCodec.sizes().plainChunks().targetBytes();
            final BytesRef[][] docSlots = randomSlots(between(1, 400), chunk);
            final List<BytesRef> flat = new ArrayList<>();
            for (BytesRef[] slots : docSlots) {
                if (slots != null) {
                    flat.addAll(Arrays.asList(slots));
                }
            }
            withColumn(docSlots, () -> new Streaming(cursor(docSlots)), withCodec, (metadata, reader) -> {
                final StringColumnMetadata.Plain plain = plainOf(metadata);
                assertFalse("a column whose writer was told it is not in order", plain.valuesSorted());
                // A value the cursor said repeats the one before is not stored again, unless it opens a block of lengths,
                // which decodes on its own, or the column is of one length and keeps no lengths to mark it with.
                final int lengthBlockSize = withCodec.sizes().lengthBlockSize();
                final boolean marksRepeats = plain.values().constant() == false;
                long storedBytes = 0;
                for (int i = 0; i < flat.size(); i++) {
                    final BytesRef value = flat.get(i);
                    final BytesRef previous = i == 0 ? null : flat.get(i - 1);
                    final boolean repeat = previous != null && value != null && previous.bytesEquals(value);
                    if (value != null && (repeat == false || marksRepeats == false || i % lengthBlockSize == 0)) {
                        storedBytes += value.length;
                    }
                }
                assertEquals(storedBytes, plain.values().chunks().uncompressedLength());
                for (int probe = 0; probe < 2 * flat.size(); probe++) {
                    final int address = probe < flat.size() ? probe : between(0, flat.size() - 1);
                    final BytesRef expected = flat.get(address);
                    final BytesRef actual = reader.valueAt(address);
                    assertEquals("value " + address, expected, actual);
                    if (expected == null) {
                        continue;
                    }
                    final ByteArrayOutputStream out = new ByteArrayOutputStream();
                    reader.transferValueAt(address, (bytes, offset, length) -> {
                        assertThat(length, lessThanOrEqualTo(chunk));
                        out.write(bytes, offset, length);
                    });
                    assertEquals("transferred " + address, expected, new BytesRef(out.toByteArray()));
                }
            });
        }
    }

    /** Documents of a few slots each: runs of one value, nulls, empty values, and values several chunks long. */
    private static BytesRef[][] randomSlots(int maxDoc, int chunk) {
        final BytesRef[][] docSlots = new BytesRef[maxDoc][];
        BytesRef previous = new BytesRef(randomByteArrayOfLength(between(1, 16)));
        for (int d = 0; d < maxDoc; d++) {
            if (rarely()) {
                continue;
            }
            docSlots[d] = new BytesRef[between(1, 3)];
            for (int s = 0; s < docSlots[d].length; s++) {
                final BytesRef value = switch (between(0, 9)) {
                    case 0 -> null;
                    case 1 -> new BytesRef();
                    case 2 -> new BytesRef(randomByteArrayOfLength(between(chunk / 2, chunk * 4)));
                    case 3, 4 -> previous == null ? new BytesRef() : BytesRef.deepCopyOf(previous);
                    default -> new BytesRef(randomByteArrayOfLength(between(1, 300)));
                };
                docSlots[d][s] = value;
                previous = value;
            }
        }
        if (numDocsWithField(docSlots) == 0) {
            docSlots[0] = new BytesRef[] { new BytesRef("x") };
        }
        return docSlots;
    }

    /** Hands values over in pieces and says which repeat, as a merge of plain columns does; asking for one whole fails. */
    private static final class Streaming extends StringColumnValues {
        private final StringColumnValues in;
        private BytesRef previous;
        private boolean hasPrevious;
        private boolean repeats;

        Streaming(StringColumnValues in) {
            this.in = in;
        }

        @Override
        public boolean streamsValues() {
            return true;
        }

        @Override
        public boolean mayBeSorted() {
            return false;
        }

        @Override
        public void nextValue() throws IOException {
            in.nextValue();
            final BytesRef value = in.value();
            repeats = hasPrevious && previous != null && value != null && previous.bytesEquals(value);
            previous = value == null ? null : BytesRef.deepCopyOf(value);
            hasPrevious = true;
        }

        @Override
        public boolean repeatsPrevious() {
            return repeats;
        }

        @Override
        public int valueLength() throws IOException {
            final BytesRef value = in.value();
            return value == null ? -1 : value.length;
        }

        @Override
        public void transferValue(BytesSink sink) throws IOException {
            final BytesRef value = in.value();
            int at = 0;
            while (at < value.length) {
                final int take = Math.min(value.length - at, between(1, 200));
                sink.append(value.bytes, value.offset + at, take);
                at += take;
            }
        }

        @Override
        public BytesRef value() {
            throw new AssertionError("a streamed value is never asked for whole");
        }

        @Override
        public int valueCount() {
            return in.valueCount();
        }

        @Override
        public int nullCount() throws IOException {
            return in.nullCount();
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return in.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return in.advance(target);
        }

        @Override
        public long cost() {
            return in.cost();
        }
    }
}
