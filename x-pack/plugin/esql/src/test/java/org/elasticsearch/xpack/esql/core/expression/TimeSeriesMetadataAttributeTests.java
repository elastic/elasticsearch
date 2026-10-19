/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesMetadataAttributeTests extends ESTestCase {
    public void testMetadataProjectionAliasesUseExistingWireFormat() {
        for (var version : List.of(FieldAttribute.ESQL_TIMESERIES_METADATA_ATTRIBUTE, TransportVersion.current())) {
            var full = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
            var withoutPod = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("pod"));
            var withoutCluster = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("cluster"));
            List<Expression> projections = List.of(
                new Alias(Source.EMPTY, "_timeseries", full),
                new Alias(Source.EMPTY, "_timeseries$1", withoutPod),
                new Alias(Source.EMPTY, "_timeseries$2", withoutCluster),
                withoutPod
            );
            List<Expression> copies = SerializationTestUtils.serializeDeserialize(projections, (out, values) -> {
                out.setTransportVersion(version);
                out.writeNamedWriteableCollection(values);
            }, in -> {
                in.setTransportVersion(version);
                return in.readNamedWriteableCollectionAsList(Expression.class);
            });
            assertEquals(projections, copies);
            for (int i = 0; i < 3; i++) {
                var original = (Alias) projections.get(i);
                var copy = (Alias) copies.get(i);
                assertEquals(original.id(), copy.id());
                var metadata = (TimeSeriesMetadataAttribute) copy.child();
                assertEquals(MetadataAttribute.TIMESERIES, metadata.name());
                assertEquals(((TimeSeriesMetadataAttribute) original.child()).excludedFields(), metadata.excludedFields());
            }
            assertSame(((Alias) copies.get(1)).child(), copies.get(3));
        }
    }

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
}
