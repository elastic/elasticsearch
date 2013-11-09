/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.support.IgnoreIndices;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 */
public class SnapshotUtilsTests {
    @Test
    public void testIndexNameFiltering() {
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{}, new String[]{"foo", "bar", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"*"}, new String[]{"foo", "bar", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"foo", "bar", "baz"}, new String[]{"foo", "bar", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"foo"}, new String[]{"foo"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"ba*", "-bar", "-baz"}, new String[]{});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"-bar"}, new String[]{"foo", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"-ba*"}, new String[]{"foo"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"+ba*"}, new String[]{"bar", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"+bar", "+foo"}, new String[]{"bar", "foo"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"zzz", "bar"}, IgnoreIndices.MISSING, new String[]{"bar"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{""}, IgnoreIndices.MISSING, new String[]{});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"foo", "", "ba*"}, IgnoreIndices.MISSING, new String[]{"foo", "bar", "baz"});
    }

    private void assertIndexNameFiltering(String[] indices, String[] filter, String[] expected) {
        assertIndexNameFiltering(indices, filter, IgnoreIndices.DEFAULT, expected);
    }

    private void assertIndexNameFiltering(String[] indices, String[] filter, IgnoreIndices ignoreIndices, String[] expected) {
        ImmutableList<String> indicesList = ImmutableList.copyOf(indices);
        ImmutableList<String> actual = SnapshotUtils.filterIndices(indicesList, filter, ignoreIndices);
        assertThat(actual, containsInAnyOrder(expected));
    }
}
