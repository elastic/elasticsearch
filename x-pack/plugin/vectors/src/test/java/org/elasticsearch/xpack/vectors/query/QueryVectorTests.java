/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class QueryVectorTests extends ESTestCase {
    public void testBadVectorType() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> QueryVector.fromObject(new HashMap<>()));
        assertThat(e.getMessage(), containsString("Cannot use vector ["));
        assertThat(e.getMessage(), containsString("] with class [java.util.HashMap] as query vector"));
    }

    public void testBadListElement() {
        List<?> vector = List.of(1.5, 2.5, "three");
        QueryVector qv = QueryVector.fromObject(vector);
        assertEquals(1.5, qv.get(0), 0.001f);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> qv.get(2));
        assertEquals("Cannot treat [three] at index [2] of type [java.lang.String] as Number", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, qv::asFloatArray);
        assertEquals("Cannot treat [three] at index [2] of type [java.lang.String] as Number", e.getMessage());
    }
}
