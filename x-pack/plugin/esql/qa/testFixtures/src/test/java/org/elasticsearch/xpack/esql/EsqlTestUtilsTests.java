/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class EsqlTestUtilsTests extends ESTestCase {

    public void testPromQL() {
        assertThat(
            EsqlTestUtils.addRemotes("PROMQL foo, bar step 1m (avg(baz))", Set.of(), false),
            equalTo("PROMQL *:foo,foo,*:bar,bar step 1m (avg(baz))")
        );
        assertThat(
            EsqlTestUtils.addRemotes("PROMQL \"foo\", \"bar\" step 1m (avg(baz))", Set.of(), false),
            equalTo("PROMQL *:foo,foo,*:bar,bar step 1m (avg(baz))")
        );
    }

    public void testPromQLDefaultIndex() {
        assertThat(EsqlTestUtils.addRemotes("PROMQL step 1m (avg(baz))", Set.of(), false), equalTo("PROMQL *:*,* step 1m (avg(baz))"));
    }

    public void testSet() {
        assertThat(
            EsqlTestUtils.addRemotes("SET a=b; FROM foo | SORT bar", Set.of(), false),
            equalTo("SET a=b; FROM *:foo,foo | SORT bar")
        );
    }

    public void testMetadata() {
        assertThat(
            EsqlTestUtils.addRemotes("FROM foo METADATA _source | SORT bar", Set.of(), false),
            equalTo("FROM *:foo,foo METADATA _source | SORT bar")
        );
    }

    public void testTS() {
        assertThat(
            EsqlTestUtils.addRemotes("TS foo, \"bar\",baz | SORT bar", Set.of(), false),
            equalTo("TS *:foo,foo,*:bar,bar,*:baz,baz | SORT bar")
        );
    }

    public void testIndexPatternWildcard() {
        assertThat(EsqlTestUtils.addRemotes("TS fo* | SORT bar", Set.of(), false), equalTo("TS *:fo*,fo* | SORT bar"));
    }

    public void testDuplicateIndex() {
        assertThat(
            EsqlTestUtils.addRemotes("TS foo,bar,foo | SORT bar", Set.of(), false),
            equalTo("TS *:foo,foo,*:bar,bar,*:foo,foo | SORT bar")
        );
    }
}
