/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlattenedFieldSyntheticWriterHelperTests extends ESTestCase {

    public void testSingleField() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        when(dv.getValueCount()).thenReturn(1L);
        when(dv.docValueCount()).thenReturn(1);
        byte[] bytes = ("test" + '\0' + "one").getBytes(StandardCharsets.UTF_8);
        when(dv.nextOrd()).thenReturn(0L);
        when(dv.lookupOrd(0L)).thenReturn(new BytesRef(bytes, 0, bytes.length));
        FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(dv);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XContentBuilder b = new XContentBuilder(XContentType.JSON.xContent(), baos);

        // WHEN
        b.startObject();
        writer.write(b);
        b.endObject();
        b.flush();

        // THEN
        assertEquals("{\"test\":\"one\"}", baos.toString(StandardCharsets.UTF_8));
    }

    public void testFlatObject() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(dv);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), baos);
        final List<byte[]> bytes = List.of("a" + '\0' + "value_a", "b" + '\0' + "value_b", "c" + '\0' + "value_c", "d" + '\0' + "value_d")
            .stream()
            .map(x -> x.getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L, 1L, 2L, 3L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }

        // WHEN
        builder.startObject();
        writer.write(builder);
        builder.endObject();
        builder.flush();

        // THEN
        assertEquals("{\"a\":\"value_a\",\"b\":\"value_b\",\"c\":\"value_c\",\"d\":\"value_d\"}", baos.toString(StandardCharsets.UTF_8));
    }

    public void testSingleObject() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(dv);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), baos);
        final List<byte[]> bytes = List.of(
            "a" + '\0' + "value_a",
            "a.b" + '\0' + "value_b",
            "a.b.c" + '\0' + "value_c",
            "a.d" + '\0' + "value_d"
        ).stream().map(x -> x.getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList());
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L, 1L, 2L, 3L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }

        // WHEN
        builder.startObject();
        writer.write(builder);
        builder.endObject();
        builder.flush();

        // THEN
        assertEquals(
            "{\"a\":\"value_a\",\"a\":{\"b\":\"value_b\",\"b\":{\"c\":\"value_c\"},\"d\":\"value_d\"}}",
            baos.toString(StandardCharsets.UTF_8)
        );
    }

    public void testMultipleObjects() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(dv);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), baos);
        final List<byte[]> bytes = List.of("a.x" + '\0' + "10", "a.y" + '\0' + "20", "b.a" + '\0' + "30", "b.c" + '\0' + "40")
            .stream()
            .map(x -> x.getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L, 1L, 2L, 3L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }

        // WHEN
        builder.startObject();
        writer.write(builder);
        builder.endObject();
        builder.flush();

        // THEN
        assertEquals("{\"a\":{\"x\":\"10\",\"y\":\"20\"},\"b\":{\"a\":\"30\",\"c\":\"40\"}}", baos.toString(StandardCharsets.UTF_8));
    }

    public void testSingleArray() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(dv);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), baos);
        final List<byte[]> bytes = List.of("a.x" + '\0' + "10", "a.x" + '\0' + "20", "a.x" + '\0' + "30", "a.x" + '\0' + "40")
            .stream()
            .map(x -> x.getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L, 1L, 2L, 3L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }

        // WHEN
        builder.startObject();
        writer.write(builder);
        builder.endObject();
        builder.flush();

        // THEN
        assertEquals("{\"a\":{\"x\":[\"10\",\"20\",\"30\",\"40\"]}}", baos.toString(StandardCharsets.UTF_8));
    }

    public void testMultipleArrays() throws IOException {
        // GIVEN
        final SortedSetDocValues dv = mock(SortedSetDocValues.class);
        final FlattenedFieldSyntheticWriterHelper writer = new FlattenedFieldSyntheticWriterHelper(dv);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), baos);
        final List<byte[]> bytes = List.of(
            "a.x" + '\0' + "10",
            "a.x" + '\0' + "20",
            "b.y" + '\0' + "30",
            "b.y" + '\0' + "40",
            "b.y" + '\0' + "50"
        ).stream().map(x -> x.getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList());
        when(dv.getValueCount()).thenReturn(Long.valueOf(bytes.size()));
        when(dv.docValueCount()).thenReturn(bytes.size());
        when(dv.nextOrd()).thenReturn(0L, 1L, 2L, 3L, 4L);
        for (int i = 0; i < bytes.size(); i++) {
            when(dv.lookupOrd(ArgumentMatchers.eq((long) i))).thenReturn(new BytesRef(bytes.get(i), 0, bytes.get(i).length));
        }

        // WHEN
        builder.startObject();
        writer.write(builder);
        builder.endObject();
        builder.flush();

        // THEN
        assertEquals("{\"a\":{\"x\":[\"10\",\"20\"]},\"b\":{\"y\":[\"30\",\"40\",\"50\"]}}", baos.toString(StandardCharsets.UTF_8));
    }
}
