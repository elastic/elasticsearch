/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geo;

import org.elasticsearch.index.mapper.FieldTypeTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public abstract class GeometryFieldTypeTestCase extends FieldTypeTestCase {

    @SuppressWarnings("unchecked")
    public static void assertGeoJsonFetch(List<?> expected, List<?> actual) {
        assertThat(actual.size(), equalTo(expected.size()));
        for (int i = 0; i < expected.size(); i++) {
            Object o = expected.get(i);
            if (o instanceof Map<?, ?> m) {
                assertGeoJsonFetch(m, (Map<?, ?>) actual.get(i));
            } else if (o instanceof List<?> l) {
                assertGeoJsonFetch(l, (List<?>) actual.get(i));
            } else {
                assertThat(o, equalTo(actual.get(i)));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void assertGeoJsonFetch(Map<?, ?> expected, Map<?, ?> actual) {
        assertThat(actual.size(), equalTo(expected.size()));
        for (Map.Entry<?, ?> e : expected.entrySet()) {
            assertTrue(actual.containsKey(e.getKey()));
            Object o = e.getValue();
            if (o instanceof Map<?, ?> m) {
                assertGeoJsonFetch(m, (Map<String, Object>) actual.get(e.getKey()));
            } else if (o instanceof List<?> l) {
                assertGeoJsonFetch(l, (List<?>) actual.get(e.getKey()));
            } else {
                assertThat(o, equalTo(actual.get(e.getKey())));
            }
        }
    }
}
