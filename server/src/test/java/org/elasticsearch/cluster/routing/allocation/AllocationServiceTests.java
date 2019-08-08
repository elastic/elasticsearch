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
package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class AllocationServiceTests extends ESTestCase {

    public void testFirstListElementsToCommaDelimitedStringReportsAllElementsIfShort() {
        List<String> strings = IntStream.range(0, between(0, 10)).mapToObj(i -> randomAlphaOfLength(10)).collect(Collectors.toList());
        assertAllElementsReported(strings, randomBoolean());
    }

    public void testFirstListElementsToCommaDelimitedStringReportsAllElementsIfDebugEnabled() {
        List<String> strings = IntStream.range(0, between(0, 100)).mapToObj(i -> randomAlphaOfLength(10)).collect(Collectors.toList());
        assertAllElementsReported(strings, true);
    }

    private void assertAllElementsReported(List<String> strings, boolean isDebugEnabled) {
        final String abbreviated = AllocationService.firstListElementsToCommaDelimitedString(strings, Function.identity(), isDebugEnabled);
        for (String string : strings) {
            assertThat(abbreviated, containsString(string));
        }
        assertThat(abbreviated, not(containsString("...")));
    }

    public void testFirstListElementsToCommaDelimitedStringReportsFirstElementsIfLong() {
        List<String> strings = IntStream.range(0, between(0, 100))
            .mapToObj(i -> randomAlphaOfLength(between(6, 10))).distinct().collect(Collectors.toList());
        final String abbreviated = AllocationService.firstListElementsToCommaDelimitedString(strings, Function.identity(), false);
        for (int i = 0; i < strings.size(); i++) {
            if (i < 10) {
                assertThat(abbreviated, containsString(strings.get(i)));
            } else {
                assertThat(abbreviated, not(containsString(strings.get(i))));
            }
        }

        if (strings.size() > 10) {
            assertThat(abbreviated, containsString("..."));
            assertThat(abbreviated, containsString("[" + strings.size() + " items in total]"));
        } else {
            assertThat(abbreviated, not(containsString("...")));
        }
    }

    public void testFirstListElementsToCommaDelimitedStringUsesFormatterNotToString() {
        List<String> strings = IntStream.range(0, between(1, 100)).mapToObj(i -> "original").collect(Collectors.toList());
        final String abbreviated = AllocationService.firstListElementsToCommaDelimitedString(strings, s -> "formatted", randomBoolean());
        assertThat(abbreviated, containsString("formatted"));
        assertThat(abbreviated, not(containsString("original")));
    }
}
