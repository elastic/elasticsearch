/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.mockito.ArgumentMatchers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlattenedFieldSyntheticWriterHelperTests extends ESTestCase {

    private String writeNested(FlattenedFieldSyntheticWriterHelper writer) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XContentBuilder b = new XContentBuilder(XContentType.JSON.xContent(), baos);
        b.startObject();
        writer.writeNested(b);
        b.endObject();
        b.flush();
        return baos.toString(StandardCharsets.UTF_8);
    }

    private String writeFlattened(FlattenedFieldSyntheticWriterHelper writer) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XContentBuilder b = new XContentBuilder(XContentType.JSON.xContent(), baos);
        b.startObject();
        writer.writeFlattened(b);
        b.endObject();
        b.flush();
        return baos.toString(StandardCharsets.UTF_8);
    }

    public void testSingleField() throws IOException {
        final SortedSetSortedKeyedValues values = mockSortedKeyedValues("test" + '\0' + "one");
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of()
        );

        assertEquals("{\"test\":\"one\"}", writeNested(writer));
        values.reset();
        assertEquals("{\"test\":\"one\"}", writeFlattened(writer));
    }

    public void testFlatObject() throws IOException {
        final SortedSetSortedKeyedValues values = mockSortedKeyedValues(
            "a" + '\0' + "value_a",
            "b" + '\0' + "value_b",
            "c" + '\0' + "value_c",
            "d" + '\0' + "value_d"
        );
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of()
        );

        assertEquals("{\"a\":\"value_a\",\"b\":\"value_b\",\"c\":\"value_c\",\"d\":\"value_d\"}", writeNested(writer));
        values.reset();
        assertEquals("{\"a\":\"value_a\",\"b\":\"value_b\",\"c\":\"value_c\",\"d\":\"value_d\"}", writeFlattened(writer));
    }

    public void testSingleObject() throws IOException {
        final SortedSetSortedKeyedValues values = mockSortedKeyedValues(
            "a" + '\0' + "value_a",
            "a.b" + '\0' + "value_b",
            "a.b.c" + '\0' + "value_c",
            "a.d" + '\0' + "value_d"
        );
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of()
        );

        assertEquals("{\"a\":\"value_a\",\"a.b\":\"value_b\",\"a.b.c\":\"value_c\",\"a.d\":\"value_d\"}", writeNested(writer));
        values.reset();
        assertEquals("{\"a\":\"value_a\",\"a.b\":\"value_b\",\"a.b.c\":\"value_c\",\"a.d\":\"value_d\"}", writeFlattened(writer));
    }

    public void testMultipleObjects() throws IOException {
        final SortedSetSortedKeyedValues values = mockSortedKeyedValues(
            "a.x" + '\0' + "10",
            "a.y" + '\0' + "20",
            "b.a" + '\0' + "30",
            "b.c" + '\0' + "40"
        );
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of()
        );

        assertEquals("{\"a\":{\"x\":\"10\",\"y\":\"20\"},\"b\":{\"a\":\"30\",\"c\":\"40\"}}", writeNested(writer));
        values.reset();
        assertEquals("{\"a.x\":\"10\",\"a.y\":\"20\",\"b.a\":\"30\",\"b.c\":\"40\"}", writeFlattened(writer));
    }

    public void testSingleArray() throws IOException {
        final SortedSetSortedKeyedValues values = mockSortedKeyedValues(
            "a.x" + '\0' + "10",
            "a.x" + '\0' + "20",
            "a.x" + '\0' + "30",
            "a.x" + '\0' + "40"
        );
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of()
        );

        assertEquals("{\"a\":{\"x\":[\"10\",\"20\",\"30\",\"40\"]}}", writeNested(writer));
        values.reset();
        assertEquals("{\"a.x\":[\"10\",\"20\",\"30\",\"40\"]}", writeFlattened(writer));
    }

    public void testMultipleArrays() throws IOException {
        final SortedSetSortedKeyedValues values = mockSortedKeyedValues(
            "a.x" + '\0' + "10",
            "a.x" + '\0' + "20",
            "b.y" + '\0' + "30",
            "b.y" + '\0' + "40",
            "b.y" + '\0' + "50"
        );
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of()
        );

        assertEquals("{\"a\":{\"x\":[\"10\",\"20\"]},\"b\":{\"y\":[\"30\",\"40\",\"50\"]}}", writeNested(writer));
        values.reset();
        assertEquals("{\"a.x\":[\"10\",\"20\"],\"b.y\":[\"30\",\"40\",\"50\"]}", writeFlattened(writer));
    }

    public void testSingleArrayWithOffsets() throws IOException {
        final List<FlattenedFieldArrayContext.KeyedOffsetField> offsets = List.of(
            new FlattenedFieldArrayContext.KeyedOffsetField("a.x", new int[] { 1, 0, -1, 1, 3, 3, 2, -1 })
        );
        int[] offsetsIdx = { 0 };

        final SortedSetSortedKeyedValues values = mockSortedKeyedValues(
            "a.x" + '\0' + "10",
            "a.x" + '\0' + "20",
            "a.x" + '\0' + "30",
            "a.x" + '\0' + "40"
        );
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            () -> offsetsIdx[0] >= offsets.size() ? null : offsets.get(offsetsIdx[0]++),
            List.of()
        );

        assertEquals("{\"a\":{\"x\":[\"20\",\"10\",null,\"20\",\"40\",\"40\",\"30\",null]}}", writeNested(writer));
        values.reset();
        offsetsIdx[0] = 0;
        assertEquals("{\"a.x\":[\"20\",\"10\",null,\"20\",\"40\",\"40\",\"30\",null]}", writeFlattened(writer));
    }

    public void testSameLeafDifferentPrefix() throws IOException {
        final SortedSetSortedKeyedValues values = mockSortedKeyedValues("a.x" + '\0' + "10", "b.x" + '\0' + "20");
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of()
        );

        assertEquals("{\"a\":{\"x\":\"10\"},\"b\":{\"x\":\"20\"}}", writeNested(writer));
        values.reset();
        assertEquals("{\"a.x\":\"10\",\"b.x\":\"20\"}", writeFlattened(writer));
    }

    public void testScalarObjectMismatchInNestedObject() throws IOException {
        final SortedSetSortedKeyedValues values = mockSortedKeyedValues("a.b.c" + '\0' + "10", "a.b.c.d" + '\0' + "20");
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of()
        );

        assertEquals("{\"a\":{\"b\":{\"c\":\"10\",\"c.d\":\"20\"}}}", writeNested(writer));
        values.reset();
        assertEquals("{\"a.b.c\":\"10\",\"a.b.c.d\":\"20\"}", writeFlattened(writer));
    }

    public void testSingleDotPath() throws IOException {
        final SortedSetSortedKeyedValues values = mockSortedKeyedValues("." + '\0' + "10");
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of()
        );

        assertEquals("{\"\":{\"\":\"10\"}}", writeNested(writer));
        values.reset();
        assertEquals("{\".\":\"10\"}", writeFlattened(writer));
    }

    public void testTrailingDotsPath() throws IOException {
        final SortedSetSortedKeyedValues values = mockSortedKeyedValues("cat.." + '\0' + "10");
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of()
        );

        assertEquals("{\"cat\":{\"\":{\"\":\"10\"}}}", writeNested(writer));
        values.reset();
        assertEquals("{\"cat..\":\"10\"}", writeFlattened(writer));
    }

    public void testFlattenedSubFieldBeforeAllDocValues() throws IOException {
        // sub-field key "a" sorts before all doc-values keys "b" and "c"
        final SortedSetSortedKeyedValues values = mockSortedKeyedValues("b" + '\0' + "dv_b", "c" + '\0' + "dv_c");
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of(Map.entry("a", fixedSubField("a", "sub_a")))
        );

        assertEquals("{\"a\":\"sub_a\",\"b\":\"dv_b\",\"c\":\"dv_c\"}", writeFlattened(writer));
    }

    public void testFlattenedSubFieldAfterAllDocValues() throws IOException {
        // sub-field key "z" sorts after all doc-values keys "a" and "b"
        final SortedSetSortedKeyedValues values = mockSortedKeyedValues("a" + '\0' + "dv_a", "b" + '\0' + "dv_b");
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of(Map.entry("z", fixedSubField("z", "sub_z")))
        );

        assertEquals("{\"a\":\"dv_a\",\"b\":\"dv_b\",\"z\":\"sub_z\"}", writeFlattened(writer));
    }

    public void testFlattenedSubFieldInterleaved() throws IOException {
        // sub-field key "b" interleaves between doc-values keys "a" and "c"
        final SortedSetSortedKeyedValues values = mockSortedKeyedValues("a" + '\0' + "dv_a", "c" + '\0' + "dv_c");
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of(Map.entry("b", fixedSubField("b", "sub_b")))
        );

        assertEquals("{\"a\":\"dv_a\",\"b\":\"sub_b\",\"c\":\"dv_c\"}", writeFlattened(writer));
    }

    public void testFlattenedMultipleSubFieldsInterleaved() throws IOException {
        // sub-fields "a" and "c" interleave around doc-values key "b";
        // writeNested appends sub-fields after all doc-values keys instead of merging
        final SortedSetSortedKeyedValues values = mockSortedKeyedValues("b" + '\0' + "dv_b");
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of(Map.entry("a", fixedSubField("a", "sub_a")), Map.entry("c", fixedSubField("c", "sub_c")))
        );

        assertEquals("{\"a\":\"sub_a\",\"b\":\"dv_b\",\"c\":\"sub_c\"}", writeFlattened(writer));
        values.reset();
        assertEquals("{\"b\":\"dv_b\",\"a\":\"sub_a\",\"c\":\"sub_c\"}", writeNested(writer));
    }

    public void testFlattenedOnlySubFields() throws IOException {
        // no doc-values keys at all; only sub-fields
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            () -> null,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
            List.of(Map.entry("a", fixedSubField("a", "sub_a")), Map.entry("b", fixedSubField("b", "sub_b")))
        );

        assertEquals("{\"a\":\"sub_a\",\"b\":\"sub_b\"}", writeFlattened(writer));
        assertEquals("{\"a\":\"sub_a\",\"b\":\"sub_b\"}", writeNested(writer));
    }

    /**
     * A minimal {@link SourceLoader.SyntheticFieldLoader} that always has a value and writes
     * a single string field {@code name: value} into the {@link XContentBuilder}.
     */
    private static SourceLoader.SyntheticFieldLoader fixedSubField(String name, String value) {
        return new SourceLoader.SyntheticFieldLoader() {
            @Override
            public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
                return Stream.of();
            }

            @Override
            public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) {
                return null;
            }

            @Override
            public boolean hasValue() {
                return true;
            }

            @Override
            public void write(XContentBuilder b) throws IOException {
                b.field(name, value);
            }

            @Override
            public void reset() {}

            @Override
            public String fieldName() {
                return name;
            }
        };
    }

    private SortedSetSortedKeyedValues mockSortedKeyedValues(String... keyValuePairs) throws IOException {
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        final List<byte[]> bytes = Stream.of(keyValuePairs).map(x -> x.getBytes(StandardCharsets.UTF_8)).toList();
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        Long[] restOrds = new Long[bytes.size() - 1];
        for (int i = 0; i < restOrds.length; i++) {
            restOrds[i] = (long) (i + 1);
        }
        when(dv.nextOrd()).thenReturn(0L, restOrds).thenReturn(0L, restOrds);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }
        return new SortedSetSortedKeyedValues(dv);
    }

    private class SortedSetSortedKeyedValues implements FlattenedFieldSyntheticWriterHelper.SortedKeyedValues {
        private final SortedSetDocValues dv;
        private int seen = 0;

        private SortedSetSortedKeyedValues(SortedSetDocValues dv) {
            this.dv = dv;
        }

        @Override
        public BytesRef next() throws IOException {
            if (seen < dv.docValueCount()) {
                seen += 1;
                return dv.lookupOrd(dv.nextOrd());
            }

            return null;
        }

        public void reset() {
            seen = 0;
        }
    }
}
