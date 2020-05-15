/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.collect;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;

public class ListTests extends ESTestCase {

    public void testStringListOfZero() {
        final String[] strings = {};
        final java.util.List<String> stringsList = List.of(strings);
        assertThat(stringsList.size(), equalTo(strings.length));
        assertTrue(stringsList.containsAll(Arrays.asList(strings)));
        expectThrows(UnsupportedOperationException.class, () -> stringsList.add("foo"));
    }

    public void testStringListOfOne() {
        final String[] strings = {"foo"};
        final java.util.List<String> stringsList = List.of(strings);
        assertThat(stringsList.size(), equalTo(strings.length));
        assertTrue(stringsList.containsAll(Arrays.asList(strings)));
        expectThrows(UnsupportedOperationException.class, () -> stringsList.add("foo"));
    }

    public void testStringListOfTwo() {
        final String[] strings = {"foo", "bar"};
        final java.util.List<String> stringsList = List.of(strings);
        assertThat(stringsList.size(), equalTo(strings.length));
        assertTrue(stringsList.containsAll(Arrays.asList(strings)));
        expectThrows(UnsupportedOperationException.class, () -> stringsList.add("foo"));
    }

    public void testStringListOfN() {
        final String[] strings = {"foo", "bar", "baz"};
        final java.util.List<String> stringsList = List.of(strings);
        assertThat(stringsList.size(), equalTo(strings.length));
        assertTrue(stringsList.containsAll(Arrays.asList(strings)));
        expectThrows(UnsupportedOperationException.class, () -> stringsList.add("foo"));
    }

    public void testCopyOf() {
        final Collection<String> coll = Arrays.asList("foo", "bar", "baz");
        final java.util.List<String> copy = List.copyOf(coll);
        assertThat(coll.size(), equalTo(copy.size()));
        assertTrue(copy.containsAll(coll));
        expectThrows(UnsupportedOperationException.class, () -> copy.add("foo"));
    }
}
