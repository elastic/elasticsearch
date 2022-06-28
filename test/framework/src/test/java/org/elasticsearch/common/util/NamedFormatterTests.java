/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;

public class NamedFormatterTests extends ESTestCase {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public void testPatternAreFormatted() {
        assertThat(NamedFormatter.format("Hello, %(name)!", singletonMap("name", "world")), equalTo("Hello, world!"));
    }

    public void testDuplicatePatternsAreFormatted() {
        assertThat(NamedFormatter.format("Hello, %(name) and %(name)!", singletonMap("name", "world")), equalTo("Hello, world and world!"));
    }

    public void testMultiplePatternsAreFormatted() {
        final Map<String, Object> values = new HashMap<>();
        values.put("name", "world");
        values.put("second_name", "fred");

        assertThat(NamedFormatter.format("Hello, %(name) and %(second_name)!", values), equalTo("Hello, world and fred!"));
    }

    public void testEscapedPatternsAreNotFormatted() {
        assertThat(NamedFormatter.format("Hello, \\%(name)!", singletonMap("name", "world")), equalTo("Hello, %(name)!"));
    }

    public void testUnknownPatternsThrowException() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("No parameter value for %(name)");
        NamedFormatter.format("Hello, %(name)!", singletonMap("foo", "world"));
    }
}
