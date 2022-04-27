/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import org.elasticsearch.core.Tuple;
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
