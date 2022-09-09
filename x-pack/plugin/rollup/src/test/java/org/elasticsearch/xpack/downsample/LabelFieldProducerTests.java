/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.search.aggregations.AggregatorTestCase;

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

    public void testLabelFieldProducer() {
        final LabelFieldProducer producer = new LabelFieldProducer.LabelLastValueFieldProducer("dummy");
        assertTrue(producer.isEmpty());
        assertEquals("dummy", producer.name());
        assertEquals("last_value", producer.label().name);
        producer.collect("aaaa");
        assertFalse(producer.isEmpty());
        assertEquals("aaaa", producer.value());
        producer.reset();
        assertTrue(producer.isEmpty());
        assertNull(producer.value());
    }
}
