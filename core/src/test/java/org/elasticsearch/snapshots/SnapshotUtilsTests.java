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
package org.elasticsearch.snapshots;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 */
public class SnapshotUtilsTests extends ESTestCase {
    public void testIndexNameFiltering() {
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{}, new String[]{"foo", "bar", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"*"}, new String[]{"foo", "bar", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"_all"}, new String[]{"foo", "bar", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"foo", "bar", "baz"}, new String[]{"foo", "bar", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"foo"}, new String[]{"foo"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"baz", "not_available"}, new String[]{"baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"ba*", "-bar", "-baz"}, new String[]{});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"-bar"}, new String[]{"foo", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"-ba*"}, new String[]{"foo"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"+ba*"}, new String[]{"bar", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"+bar", "+foo"}, new String[]{"bar", "foo"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"zzz", "bar"}, IndicesOptions.lenientExpandOpen(), new String[]{"bar"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{""}, IndicesOptions.lenientExpandOpen(), new String[]{});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"foo", "", "ba*"}, IndicesOptions.lenientExpandOpen(), new String[]{"foo", "bar", "baz"});
    }

    private void assertIndexNameFiltering(String[] indices, String[] filter, String[] expected) {
        assertIndexNameFiltering(indices, filter, IndicesOptions.lenientExpandOpen(), expected);
    }

    private void assertIndexNameFiltering(String[] indices, String[] filter, IndicesOptions indicesOptions, String[] expected) {
        List<String> indicesList = Arrays.asList(indices);
        List<String> actual = SnapshotUtils.filterIndices(indicesList, filter, indicesOptions);
        assertThat(actual, containsInAnyOrder(expected));
    }
}
