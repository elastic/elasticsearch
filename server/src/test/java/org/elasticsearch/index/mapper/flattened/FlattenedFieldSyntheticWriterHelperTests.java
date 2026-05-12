/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.mockito.ArgumentMatchers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlattenedFieldSyntheticWriterHelperTests extends ESTestCase {

    private String write(FlattenedFieldSyntheticWriterHelper writer, FlattenedFieldSyntheticWriterHelper.OutputStructure outputStructure)
        throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XContentBuilder b = new XContentBuilder(XContentType.JSON.xContent(), baos);

        // WHEN
        b.startObject();
        writer.write(b, outputStructure);
        b.endObject();
        b.flush();

        return baos.toString(StandardCharsets.UTF_8);
    }

    public void testSingleField() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        when(dv.getValueCount()).thenReturn(1L);
        when(dv.docValueCount()).thenReturn(1);
        byte[] bytes = ("test" + '\0' + "one").getBytes(StandardCharsets.UTF_8);
        when(dv.nextOrd()).thenReturn(0L).thenReturn(0L);
        when(dv.lookupOrd(0L)).thenReturn(new BytesRef(bytes, 0, bytes.length));
        var values = new SortedSetSortedKeyedValues(dv);
        FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE
        );

        // THEN
        assertEquals("{\"test\":\"one\"}", write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.NESTED));
        values.reset();
        assertEquals("{\"test\":\"one\"}", write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.FLATTENED));
    }

    public void testFlatObject() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        var values = new SortedSetSortedKeyedValues(dv);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE
        );
        final List<byte[]> bytes = List.of("a" + '\0' + "value_a", "b" + '\0' + "value_b", "c" + '\0' + "value_c", "d" + '\0' + "value_d")
            .stream()
            .map(x -> x.getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L, 1L, 2L, 3L).thenReturn(0L, 1L, 2L, 3L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }

        // THEN
        assertEquals(
            "{\"a\":\"value_a\",\"b\":\"value_b\",\"c\":\"value_c\",\"d\":\"value_d\"}",
            write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.NESTED)
        );
        values.reset();
        assertEquals(
            "{\"a\":\"value_a\",\"b\":\"value_b\",\"c\":\"value_c\",\"d\":\"value_d\"}",
            write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.FLATTENED)
        );
    }

    public void testSingleObject() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        var values = new SortedSetSortedKeyedValues(dv);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE
        );
        final List<byte[]> bytes = List.of(
            "a" + '\0' + "value_a",
            "a.b" + '\0' + "value_b",
            "a.b.c" + '\0' + "value_c",
            "a.d" + '\0' + "value_d"
        ).stream().map(x -> x.getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList());
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L, 1L, 2L, 3L).thenReturn(0L, 1L, 2L, 3L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }

        // THEN
        assertEquals(
            "{\"a\":\"value_a\",\"a.b\":\"value_b\",\"a.b.c\":\"value_c\",\"a.d\":\"value_d\"}",
            write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.NESTED)
        );
        values.reset();
        assertEquals(
            "{\"a\":\"value_a\",\"a.b\":\"value_b\",\"a.b.c\":\"value_c\",\"a.d\":\"value_d\"}",
            write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.FLATTENED)
        );
    }

    public void testMultipleObjects() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        var values = new SortedSetSortedKeyedValues(dv);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE
        );
        final List<byte[]> bytes = List.of("a.x" + '\0' + "10", "a.y" + '\0' + "20", "b.a" + '\0' + "30", "b.c" + '\0' + "40")
            .stream()
            .map(x -> x.getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L, 1L, 2L, 3L).thenReturn(0L, 1L, 2L, 3L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }

        // THEN
        assertEquals(
            "{\"a\":{\"x\":\"10\",\"y\":\"20\"},\"b\":{\"a\":\"30\",\"c\":\"40\"}}",
            write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.NESTED)
        );
        values.reset();
        assertEquals(
            "{\"a.x\":\"10\",\"a.y\":\"20\",\"b.a\":\"30\",\"b.c\":\"40\"}",
            write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.FLATTENED)
        );
    }

    public void testSingleArray() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        var values = new SortedSetSortedKeyedValues(dv);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE
        );
        final List<byte[]> bytes = List.of("a.x" + '\0' + "10", "a.x" + '\0' + "20", "a.x" + '\0' + "30", "a.x" + '\0' + "40")
            .stream()
            .map(x -> x.getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L, 1L, 2L, 3L).thenReturn(0L, 1L, 2L, 3L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }
        // THEN
        assertEquals(
            "{\"a\":{\"x\":[\"10\",\"20\",\"30\",\"40\"]}}",
            write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.NESTED)
        );
        values.reset();
        assertEquals(
            "{\"a.x\":[\"10\",\"20\",\"30\",\"40\"]}",
            write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.FLATTENED)
        );
    }

    public void testMultipleArrays() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        var values = new SortedSetSortedKeyedValues(dv);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE
        );
        final List<byte[]> bytes = List.of(
            "a.x" + '\0' + "10",
            "a.x" + '\0' + "20",
            "b.y" + '\0' + "30",
            "b.y" + '\0' + "40",
            "b.y" + '\0' + "50"
        ).stream().map(x -> x.getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList());
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L, 1L, 2L, 3L, 4L).thenReturn(0L, 1L, 2L, 3L, 4L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }

        // THEN
        assertEquals(
            "{\"a\":{\"x\":[\"10\",\"20\"]},\"b\":{\"y\":[\"30\",\"40\",\"50\"]}}",
            write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.NESTED)
        );
        values.reset();
        assertEquals(
            "{\"a.x\":[\"10\",\"20\"],\"b.y\":[\"30\",\"40\",\"50\"]}",
            write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.FLATTENED)
        );
    }

    public void testSingleArrayWithOffsets() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);

        final var offsets = List.of(new FlattenedFieldArrayContext.KeyedOffsetField("a.x", new int[] { 1, 0, -1, 1, 3, 3, 2, -1 }));
        int[] offsetsIdx = { 0 };

        var values = new SortedSetSortedKeyedValues(dv);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            () -> offsetsIdx[0] >= offsets.size() ? null : offsets.get(offsetsIdx[0]++)
        );
        final List<byte[]> bytes = List.of("a.x" + '\0' + "10", "a.x" + '\0' + "20", "a.x" + '\0' + "30", "a.x" + '\0' + "40")
            .stream()
            .map(x -> x.getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L, 1L, 2L, 3L).thenReturn(0L, 1L, 2L, 3L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }

        // THEN
        assertEquals(
            "{\"a\":{\"x\":[\"20\",\"10\",null,\"20\",\"40\",\"40\",\"30\",null]}}",
            write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.NESTED)
        );
        values.reset();
        offsetsIdx[0] = 0;
        assertEquals(
            "{\"a.x\":[\"20\",\"10\",null,\"20\",\"40\",\"40\",\"30\",null]}",
            write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.FLATTENED)
        );
    }

    public void testSameLeafDifferentPrefix() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        var values = new SortedSetSortedKeyedValues(dv);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE
        );
        final List<byte[]> bytes = List.of("a.x" + '\0' + "10", "b.x" + '\0' + "20")
            .stream()
            .map(x -> x.getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L, 1L).thenReturn(0L, 1L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }

        // THEN
        assertEquals(
            "{\"a\":{\"x\":\"10\"},\"b\":{\"x\":\"20\"}}",
            write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.NESTED)
        );
        values.reset();
        assertEquals("{\"a.x\":\"10\",\"b.x\":\"20\"}", write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.FLATTENED));
    }

    public void testScalarObjectMismatchInNestedObject() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        var values = new SortedSetSortedKeyedValues(dv);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE
        );
        final List<byte[]> bytes = List.of("a.b.c" + '\0' + "10", "a.b.c.d" + '\0' + "20")
            .stream()
            .map(x -> x.getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L, 1L).thenReturn(0L, 1L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }

        // THEN
        assertEquals(
            "{\"a\":{\"b\":{\"c\":\"10\",\"c.d\":\"20\"}}}",
            write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.NESTED)
        );
        values.reset();
        assertEquals("{\"a.b.c\":\"10\",\"a.b.c.d\":\"20\"}", write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.FLATTENED));
    }

    public void testSingleDotPath() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        var values = new SortedSetSortedKeyedValues(dv);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE
        );
        final List<byte[]> bytes = Stream.of("." + '\0' + "10").map(x -> x.getBytes(StandardCharsets.UTF_8)).toList();
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L).thenReturn(0L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }

        // THEN
        assertEquals("{\"\":{\"\":\"10\"}}", write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.NESTED));
        values.reset();
        assertEquals("{\".\":\"10\"}", write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.FLATTENED));
    }

    public void testTrailingDotsPath() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        var values = new SortedSetSortedKeyedValues(dv);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(
            values,
            FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE
        );
        final List<byte[]> bytes = Stream.of("cat.." + '\0' + "10").map(x -> x.getBytes(StandardCharsets.UTF_8)).toList();
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L).thenReturn(0L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }

        // THEN
        assertEquals("{\"cat\":{\"\":{\"\":\"10\"}}}", write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.NESTED));
        values.reset();
        assertEquals("{\"cat..\":\"10\"}", write(writer, FlattenedFieldSyntheticWriterHelper.OutputStructure.FLATTENED));
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
