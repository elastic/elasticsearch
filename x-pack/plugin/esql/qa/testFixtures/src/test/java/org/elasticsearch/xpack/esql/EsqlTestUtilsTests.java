/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.PromqlFeatures;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class EsqlTestUtilsTests extends ESTestCase {

    public void testPromQL() {
        assumeTrue("requires snapshot build with promql feature enabled", PromqlFeatures.isEnabled());
        assertThat(
            EsqlTestUtils.addRemoteIndices("PROMQL foo, bar step=1m (avg(baz))", Set.of(), false),
            equalTo("PROMQL *:foo,foo,*:bar,bar step=1m (avg(baz))")
        );
        assertThat(
            EsqlTestUtils.addRemoteIndices("PROMQL \"foo\", \"bar\" step=1m (avg(baz))", Set.of(), false),
            equalTo("PROMQL *:foo,foo,*:bar,bar step=1m (avg(baz))")
        );
    }

    public void testPromQLDefaultIndex() {
        assumeTrue("requires snapshot build with promql feature enabled", PromqlFeatures.isEnabled());
        assertThat(
            EsqlTestUtils.addRemoteIndices("PROMQL step=1m (avg(baz))", Set.of(), false),
            equalTo("PROMQL *:*,* step=1m (avg(baz))")
        );
    }

    public void testSet() {
        assertThat(
            EsqlTestUtils.addRemoteIndices("SET a=b; FROM foo | SORT bar", Set.of(), false),
            equalTo("SET a=b; FROM *:foo,foo | SORT bar")
        );
    }

    public void testMetadata() {
        assertThat(
            EsqlTestUtils.addRemoteIndices("FROM foo METADATA _source | SORT bar", Set.of(), false),
            equalTo("FROM *:foo,foo METADATA _source | SORT bar")
        );
    }

    public void testTS() {
        assertThat(
            EsqlTestUtils.addRemoteIndices("TS foo, \"bar\",baz | SORT bar", Set.of(), false),
            equalTo("TS *:foo,foo,*:bar,bar,*:baz,baz | SORT bar")
        );
    }

    public void testIndexPatternWildcard() {
        assertThat(EsqlTestUtils.addRemoteIndices("TS fo* | SORT bar", Set.of(), false), equalTo("TS *:fo*,fo* | SORT bar"));
    }

    public void testDuplicateIndex() {
        assertThat(
            EsqlTestUtils.addRemoteIndices("TS foo,bar,foo | SORT bar", Set.of(), false),
            equalTo("TS *:foo,foo,*:bar,bar,*:foo,foo | SORT bar")
        );
    }

    public void testSubquery() {
        assertThat(EsqlTestUtils.addRemoteIndices("""
            FROM employees, (FROM employees_incompatible
                             | ENRICH languages_policy on languages with language_name )
                       metadata _index
            | EVAL emp_no = emp_no::long
            | WHERE emp_no >= 10091 AND emp_no < 10094
            | SORT _index, emp_no
            | KEEP _index,  emp_no, languages, language_name""", Set.of(), false), equalTo("""
            FROM *:employees,employees, (FROM employees_incompatible
                             | ENRICH languages_policy on languages with language_name )
                       metadata _index
            | EVAL emp_no = emp_no::long
            | WHERE emp_no >= 10091 AND emp_no < 10094
            | SORT _index, emp_no
            | KEEP _index,  emp_no, languages, language_name"""));
    }

    public void testTripleQuotes() {
        assertThat(
            EsqlTestUtils.addRemoteIndices("from \"\"\"employees\"\"\" | limit 2", Set.of(), false),
            equalTo("from *:employees,employees | limit 2")
        );
    }

    public void testRow() {
        assertThat(EsqlTestUtils.addRemoteIndices("""
            ROW a = "1953-01-23T12:15:00Z - some text - 127.0.0.1;"\s
             | DISSECT a "%{Y}-%{M}-%{D}T%{h}:%{m}:%{s}Z - %{msg} - %{ip};"\s
             | KEEP Y, M, D, h, m, s, msg, ip""", Set.of(), false), equalTo("""
            ROW a = "1953-01-23T12:15:00Z - some text - 127.0.0.1;"\s
             | DISSECT a "%{Y}-%{M}-%{D}T%{h}:%{m}:%{s}Z - %{msg} - %{ip};"\s
             | KEEP Y, M, D, h, m, s, msg, ip"""));
    }
}
