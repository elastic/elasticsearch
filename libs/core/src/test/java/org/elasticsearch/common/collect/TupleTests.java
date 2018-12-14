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

package org.elasticsearch.common.collect;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class TupleTests extends ESTestCase {

    public void testTuple() {
        Tuple<Long, String> t1 = new Tuple<>(2L, "foo");
        Tuple<Long, String> t2 = new Tuple<>(2L, "foo");
        Tuple<Long, String> t3 = new Tuple<>(3L, "foo");
        Tuple<Long, String> t4 = new Tuple<>(2L, "bar");
        Tuple<Integer, String> t5 = new Tuple<>(2, "foo");

        assertThat(t1.v1(), equalTo(Long.valueOf(2L)));
        assertThat(t1.v2(), equalTo("foo"));

        assertThat(t1, equalTo(t2));
        assertNotEquals(t1, t3);
        assertNotEquals(t2, t3);
        assertNotEquals(t2, t4);
        assertNotEquals(t3, t4);
        assertNotEquals(t1, t5);

        assertThat(t1.toString(), equalTo("Tuple [v1=2, v2=foo]"));
    }
}
