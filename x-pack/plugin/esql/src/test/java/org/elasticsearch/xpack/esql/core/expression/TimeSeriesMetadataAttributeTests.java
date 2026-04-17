/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesMetadataAttributeTests extends ESTestCase {
    public void testToString() {
        TimeSeriesMetadataAttribute attr = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        assertThat(attr.toString(), equalTo("_timeseries{f}#" + attr.id()));
    }

    public void testWithoutFields() {
        Set<String> without = Set.of("foo", "bar");
        TimeSeriesMetadataAttribute attr = new TimeSeriesMetadataAttribute(Source.EMPTY, without);
        assertEquals(without, attr.withoutFields());
    }

    public void testEqualsSameId() {
        TimeSeriesMetadataAttribute a = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("x"));
        TimeSeriesMetadataAttribute b = (TimeSeriesMetadataAttribute) a.withId(a.id());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testNotEqualWithDifferentWithoutFields() {
        TimeSeriesMetadataAttribute a = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("x"));
        TimeSeriesMetadataAttribute b = TimeSeriesMetadataAttribute.from(a, Set.of("y"));
        assertNotEquals(a, b);
    }

    public void testFrom() {
        TimeSeriesMetadataAttribute original = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("x"));
        Set<String> newWithout = Set.of("y");
        TimeSeriesMetadataAttribute derived = TimeSeriesMetadataAttribute.from(original, newWithout);
        assertEquals(newWithout, derived.withoutFields());
        assertEquals(original.id(), derived.id());
    }

    public void testFromReturnsSameInstanceWhenUnchanged() {
        Set<String> without = Set.of("x");
        TimeSeriesMetadataAttribute original = new TimeSeriesMetadataAttribute(Source.EMPTY, without);
        assertSame(original, TimeSeriesMetadataAttribute.from(original, without));
    }
}
