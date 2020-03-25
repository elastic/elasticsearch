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

import static org.hamcrest.CoreMatchers.equalTo;

public class ListTests extends ESTestCase {

    public void testStringList() {
        final String[] strings = {"foo", "bar", "baz"};
        final java.util.List<?> stringsList = List.of(strings);
        assertThat(stringsList.size(), equalTo(strings.length));
        for (int k = 0; k < strings.length; k++) {
            assertEquals(String.class, stringsList.get(k).getClass());
            assertEquals(strings[k], stringsList.get(k));
        }
    }

    public void testListIsImmutable() {
        java.util.List<?> list = List.of(new Object());
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> list.remove(0));
        assertNotNull(e);
    }
}
