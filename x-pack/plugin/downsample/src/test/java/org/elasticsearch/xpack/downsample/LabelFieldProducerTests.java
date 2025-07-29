/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LabelFieldProducerTests extends AggregatorTestCase {

    public void testLastValueKeywordLabel() {
        final LabelFieldProducer.Label label = new LabelFieldProducer.LastValueLabel();
        label.collect("aaa");
        label.collect("bbb");
        label.collect("ccc");
        assertEquals("aaa", label.get());
        label.reset();
        assertNull(label.get());
    }

    public void testLastValueDoubleLabel() {
        final LabelFieldProducer.Label label = new LabelFieldProducer.LastValueLabel();
        label.collect(10.20D);
        label.collect(17.30D);
        label.collect(12.60D);
        assertEquals(10.20D, label.get());
        label.reset();
        assertNull(label.get());
    }

    public void testLastValueIntegerLabel() {
        final LabelFieldProducer.Label label = new LabelFieldProducer.LastValueLabel();
        label.collect(10);
        label.collect(17);
        label.collect(12);
        assertEquals(10, label.get());
        label.reset();
        assertNull(label.get());
    }

    public void testLastValueLongLabel() {
        final LabelFieldProducer.Label label = new LabelFieldProducer.LastValueLabel();
        label.collect(10L);
        label.collect(17L);
        label.collect(12L);
        assertEquals(10L, label.get());
        label.reset();
        assertNull(label.get());
    }

    public void testLastValueBooleanLabel() {
        final LabelFieldProducer.Label label = new LabelFieldProducer.LastValueLabel();
        label.collect(true);
        label.collect(false);
        label.collect(true);
        assertEquals(true, label.get());
        label.reset();
        assertNull(label.get());
    }

    public void testLabelFieldProducer() throws IOException {
        final LabelFieldProducer producer = new LabelFieldProducer.LabelLastValueFieldProducer("dummy");
        assertTrue(producer.isEmpty());
        assertEquals("dummy", producer.name());
        assertEquals("last_value", producer.label().name());
        FormattedDocValues docValues = new FormattedDocValues() {
            @Override
            public boolean advanceExact(int docId) {
                return true;
            }

            @Override
            public int docValueCount() {
                return 1;
            }

            @Override
            public Object nextValue() {
                return "aaaa";
            }
        };
        producer.collect(docValues, IntArrayList.from(1));
        // producer.collect("dummy", "aaaa");
        assertFalse(producer.isEmpty());
        assertEquals("aaaa", producer.label().get());
        producer.reset();
        assertTrue(producer.isEmpty());
        assertNull(producer.label().get());
    }

    public void testFlattenedLastValueFieldProducer() throws IOException {
        var producer = new LabelFieldProducer.FlattenedLastValueFieldProducer("dummy");
        assertTrue(producer.isEmpty());
        assertEquals("dummy", producer.name());
        assertEquals("last_value", producer.label().name());

        var bytes = List.of("a\0value_a", "b\0value_b", "c\0value_c", "d\0value_d");
        var docValues = new FormattedDocValues() {

            Iterator<String> iterator = bytes.iterator();

            @Override
            public boolean advanceExact(int docId) {
                return true;
            }

            @Override
            public int docValueCount() {
                return bytes.size();
            }

            @Override
            public Object nextValue() {
                return iterator.next();
            }
        };

        producer.collect(docValues, IntArrayList.from(1));
        assertFalse(producer.isEmpty());
        assertEquals("a\0value_a", (((Object[]) producer.label().get())[0]).toString());
        assertEquals("b\0value_b", (((Object[]) producer.label().get())[1]).toString());
        assertEquals("c\0value_c", (((Object[]) producer.label().get())[2]).toString());
        assertEquals("d\0value_d", (((Object[]) producer.label().get())[3]).toString());

        var builder = new XContentBuilder(XContentType.JSON.xContent(), new ByteArrayOutputStream());
        builder.startObject();
        producer.write(builder);
        builder.endObject();
        var content = Strings.toString(builder);
        assertThat(content, equalTo("{\"dummy\":{\"a\":\"value_a\",\"b\":\"value_b\",\"c\":\"value_c\",\"d\":\"value_d\"}}"));

        producer.reset();
        assertTrue(producer.isEmpty());
        assertNull(producer.label().get());
    }
}
