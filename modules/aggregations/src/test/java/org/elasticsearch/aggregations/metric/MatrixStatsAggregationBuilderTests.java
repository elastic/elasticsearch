/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.aggregations.metric;

import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class MatrixStatsAggregationBuilderTests extends ESTestCase {

    /**
     * {@code missingMap} is the per-field "missing" value map that is serialized by {@link
     * ArrayValuesSourceAggregationBuilder#doWriteTo} (via {@code writeGenericMap(missingMap)}) and read back on the other side, so
     * two builders that differ only in {@code missingMap} represent different requests and must not be {@link Object#equals equal}.
     */
    public void testEqualsAndHashCodeConsiderMissingMap() {
        MatrixStatsAggregationBuilder a = new MatrixStatsAggregationBuilder("matrix").missingMap(Map.of("field", 1));
        MatrixStatsAggregationBuilder b = new MatrixStatsAggregationBuilder("matrix").missingMap(Map.of("field", 2));
        assertNotEquals(a, b);
        assertNotEquals(a.hashCode(), b.hashCode());

        // Builders that agree on missingMap are equal and, per the hashCode contract, must hash consistently.
        MatrixStatsAggregationBuilder aCopy = new MatrixStatsAggregationBuilder("matrix").missingMap(Map.of("field", 1));
        assertEquals(a, aCopy);
        assertEquals(a.hashCode(), aCopy.hashCode());
    }

    /**
     * {@code multiValueMode} is a field of {@link MatrixStatsAggregationBuilder} that is serialized by {@code innerWriteTo},
     * copied on clone and rendered in XContent, so two builders that differ only in {@code multiValueMode} represent different
     * requests and must not be {@link Object#equals equal}. The builder must therefore override equals()/hashCode() rather than
     * inherit the parent's, which has no knowledge of {@code multiValueMode}.
     */
    public void testEqualsAndHashCodeConsiderMultiValueMode() {
        MatrixStatsAggregationBuilder a = new MatrixStatsAggregationBuilder("matrix").multiValueMode(MultiValueMode.MIN);
        MatrixStatsAggregationBuilder b = new MatrixStatsAggregationBuilder("matrix").multiValueMode(MultiValueMode.MAX);
        assertNotEquals(a, b);
        assertNotEquals(a.hashCode(), b.hashCode());

        // Builders that agree on multiValueMode are equal and, per the hashCode contract, must hash consistently.
        MatrixStatsAggregationBuilder aCopy = new MatrixStatsAggregationBuilder("matrix").multiValueMode(MultiValueMode.MIN);
        assertEquals(a, aCopy);
        assertEquals(a.hashCode(), aCopy.hashCode());
    }

}
