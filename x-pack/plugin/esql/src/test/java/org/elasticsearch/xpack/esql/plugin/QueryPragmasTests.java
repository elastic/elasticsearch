/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class QueryPragmasTests extends ESTestCase {

    public void testTaskConcurrency() {
        assertThat(QueryPragmas.allocatedProcessorsMinusOneMaxTen(1), equalTo(2));
        assertThat(QueryPragmas.allocatedProcessorsMinusOneMaxTen(2), equalTo(2));
        assertThat(QueryPragmas.allocatedProcessorsMinusOneMaxTen(3), equalTo(2));
        assertThat(QueryPragmas.allocatedProcessorsMinusOneMaxTen(4), equalTo(3));
        assertThat(QueryPragmas.allocatedProcessorsMinusOneMaxTen(5), equalTo(4));
        assertThat(QueryPragmas.allocatedProcessorsMinusOneMaxTen(6), equalTo(5));
        assertThat(QueryPragmas.allocatedProcessorsMinusOneMaxTen(7), equalTo(6));
        assertThat(QueryPragmas.allocatedProcessorsMinusOneMaxTen(8), equalTo(7));
        assertThat(QueryPragmas.allocatedProcessorsMinusOneMaxTen(9), equalTo(8));
        assertThat(QueryPragmas.allocatedProcessorsMinusOneMaxTen(10), equalTo(9));
        assertThat(QueryPragmas.allocatedProcessorsMinusOneMaxTen(11), equalTo(10));
        for (int i = 12; i < 64; i++) {
            assertThat(QueryPragmas.allocatedProcessorsMinusOneMaxTen(i), equalTo(10));
        }
        assertThat(QueryPragmas.allocatedProcessorsMinusOneMaxTen(between(12, 4096)), equalTo(10));
    }
}
