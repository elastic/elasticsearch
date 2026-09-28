/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesMetadataAttributeTests extends ESTestCase {
    public void testToString() {
        TimeSeriesMetadataAttribute attr = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        assertThat(attr.toString(), equalTo("_timeseries{f}#" + attr.id()));
    }

    public void testExcludedFields() {
        Set<String> without = Set.of("foo", "bar");
        TimeSeriesMetadataAttribute attr = new TimeSeriesMetadataAttribute(Source.EMPTY, without);
        assertEquals(without, attr.excludedFields());
    }

    public void testEqualsSameId() {
        TimeSeriesMetadataAttribute a = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("x"));
        TimeSeriesMetadataAttribute b = (TimeSeriesMetadataAttribute) a.withId(a.id());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testNotEqualWithDifferentExcludedFields() {
        TimeSeriesMetadataAttribute a = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("x"));
        TimeSeriesMetadataAttribute b = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("y"));
        assertNotEquals(a, b);
    }

    /** Projection names are used for column alignment, so delimiter characters must not alias different projections. */
    public void testProjectionNamesAreUnambiguous() {
        assertNotEquals(TimeSeriesMetadataAttribute.nameFor(Set.of("a$b")), TimeSeriesMetadataAttribute.nameFor(Set.of("a", "b")));
        assertNotEquals(TimeSeriesMetadataAttribute.nameFor(Set.of("a$b")), TimeSeriesMetadataAttribute.nameFor(Set.of("a%24b")));
        assertNotEquals(TimeSeriesMetadataAttribute.nameFor(Set.of("")), TimeSeriesMetadataAttribute.nameFor(Set.of()));
        assertNotEquals(TimeSeriesMetadataAttribute.nameFor(Set.of("")), TimeSeriesMetadataAttribute.nameFor(Set.of("%")));
        assertEquals(
            TimeSeriesMetadataAttribute.nameFor(new LinkedHashSet<>(List.of("地域", "service.name", "a$b"))),
            TimeSeriesMetadataAttribute.nameFor(new LinkedHashSet<>(List.of("a$b", "service.name", "地域")))
        );
        assertTrue(MetadataAttribute.isTimeSeriesAttributeName(TimeSeriesMetadataAttribute.nameFor(Set.of("a$b"))));
    }

    /** Old readers need the bare metadata marker; both wire formats must retain exclusions and attribute identity. */
    public void testProjectionNameTransportCompatibility() {
        var attribute = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("a$b", "service.name"));
        for (var version : List.of(
            TransportVersion.current(),
            TransportVersionUtils.getPreviousVersion(FieldAttribute.ESQL_PROMQL_LABEL_RECORD)
        )) {
            Expression copy = SerializationTestUtils.serializeDeserialize(attribute, (out, value) -> {
                out.setTransportVersion(version);
                out.writeNamedWriteable(value);
                out.writeInt(42);
            }, in -> {
                in.setTransportVersion(version);
                Expression value = in.readNamedWriteable(Expression.class);
                assertEquals(42, in.readInt());
                assertEquals(-1, in.read());
                return value;
            });
            var restored = (TimeSeriesMetadataAttribute) copy;
            assertEquals(attribute.id(), restored.id());
            assertEquals(attribute.excludedFields(), restored.excludedFields());
            assertEquals(
                version.supports(FieldAttribute.ESQL_PROMQL_LABEL_RECORD) ? attribute.name() : MetadataAttribute.TIMESERIES,
                restored.name()
            );
        }
    }
}
