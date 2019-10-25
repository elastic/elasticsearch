/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

        assertThat(
            NamedFormatter.format("Hello, %(name) and %(second_name)!", values),
            equalTo("Hello, world and fred!")
        );
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
