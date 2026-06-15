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
            EsqlTestUtils.addRemoteIndices("PROMQL index=foo,bar step=1m (avg(foo_bar))", Set.of(), false),
            equalTo("PROMQL index=*:foo,foo,*:bar,bar step=1m (avg(foo_bar))")
        );
        assertThat(
            EsqlTestUtils.addRemoteIndices("PROMQL index=foo, bar step=1m (avg(foo_bar))", Set.of(), false),
            equalTo("PROMQL index=*:foo,foo, *:bar,bar step=1m (avg(foo_bar))")
        );
        assertThat(
            EsqlTestUtils.addRemoteIndices("PROMQL index=\"foo,bar\",\"baz\" step=1m (avg(foo_bar))", Set.of(), false),
            equalTo("PROMQL index=\"*:foo,foo,*:bar,bar\",\"*:baz,baz\" step=1m (avg(foo_bar))")
        );
        assertThat(
            EsqlTestUtils.addRemoteIndices("PROMQL step=1m index=foo,bar (avg(foo_bar))", Set.of(), false),
            equalTo("PROMQL step=1m index=*:foo,foo,*:bar,bar (avg(foo_bar))")
        );
        assertThat(
            EsqlTestUtils.addRemoteIndices("PROMQL index=\"foo\",\"bar\" step=1m (avg(foo_bar))", Set.of(), false),
            equalTo("PROMQL index=\"*:foo,foo\",\"*:bar,bar\" step=1m (avg(foo_bar))")
        );
    }

    public void testPromQLDefaultIndex() {
        assertThat(
            EsqlTestUtils.addRemoteIndices("PROMQL step=1m (avg(baz))", Set.of(), false),
            equalTo("PROMQL index=*:metrics-*,metrics-* step=1m (avg(baz))")
        );
    }

    public void testSet() {
        assertThat(
            EsqlTestUtils.addRemoteIndices("SET a=b; FROM foo | SORT bar", Set.of(), false),
            equalTo("SET a=b; FROM *:foo,foo | SORT bar")
        );
    }

    public void testSetMultiline() {
        assertThat(EsqlTestUtils.addRemoteIndices("""
            SET a=b;
            SET c=d;
            FROM foo
            | SORT bar
            """, Set.of(), false), equalTo("""
            SET a=b;
            SET c=d;
            FROM *:foo,foo
            | SORT bar
            """));
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
            equalTo("TS *:foo,foo, \"*:bar,bar\",*:baz,baz | SORT bar")
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

    public void testSubqueryWithSet() {
        assertThat(EsqlTestUtils.addRemoteIndices("""
            SET a = b;
            SET x = y; FROM employees, (FROM employees_incompatible
                             | ENRICH languages_policy on languages with language_name )
                       metadata _index
            | EVAL emp_no = emp_no::long
            """, Set.of(), false), equalTo("""
            SET a = b;
            SET x = y; FROM *:employees,employees, (FROM employees_incompatible
                             | ENRICH languages_policy on languages with language_name )
                       metadata _index
            | EVAL emp_no = emp_no::long
            """));
    }

    public void testTripleQuotes() {
        assertThat(
            EsqlTestUtils.addRemoteIndices("from \"\"\"employees\"\"\" | limit 2", Set.of(), false),
            equalTo("from \"\"\"*:employees,employees\"\"\" | limit 2")
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

    public void testOverlap() {
        assertThat(
            EsqlTestUtils.addRemoteIndices("FROM sample_data_ts_nanos, sample_data", Set.of(), false),
            equalTo("FROM *:sample_data_ts_nanos,sample_data_ts_nanos, *:sample_data,sample_data")
        );
    }

    public void testConvertSubqueryToRemoteIndicesRowSubqueryBodyUnchanged() {
        String in = """
            FROM (ROW emp_no = 99999, languages = 99)
            | KEEP emp_no, languages""";
        String out = "FROM (ROW emp_no = 99999, languages = 99) | KEEP emp_no, languages";
        assertThat(EsqlTestUtils.convertSubqueryToRemoteIndices(in), equalTo(out));
    }

    public void testConvertSubqueryToRemoteIndicesRowSubqueryWithIndexPattern() {
        String in = """
            FROM employees, (ROW emp_no = 99999)
            | KEEP emp_no""";
        String out = "FROM *:employees,employees, (ROW emp_no = 99999) | KEEP emp_no";
        assertThat(EsqlTestUtils.convertSubqueryToRemoteIndices(in), equalTo(out));
    }

    public void testConvertSubqueryToRemoteIndicesMultipleRowSubqueries() {
        String in = """
            FROM
                (ROW emp_no = 1, languages = 5),
                (ROW emp_no = 2, languages = 10)
            | SORT emp_no
            | KEEP emp_no, languages""";
        String out = "FROM (ROW emp_no = 1, languages = 5), (ROW emp_no = 2, languages = 10) | SORT emp_no | KEEP emp_no, languages";
        assertThat(EsqlTestUtils.convertSubqueryToRemoteIndices(in), equalTo(out));
    }

    public void testConvertSubqueryToRemoteIndicesFromOnly() {
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices("FROM employees, (FROM employees_incompatible | KEEP emp_no) | SORT emp_no"),
            equalTo("FROM *:employees,employees, (FROM *:employees_incompatible,employees_incompatible | KEEP emp_no) | SORT emp_no")
        );
    }

    public void testConvertSubqueryToRemoteIndicesTsSubquery() {
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices(
                "FROM sample_data, (TS k8s | STATS m=max(rate(network.total_bytes_in)) BY cluster) | KEEP cluster, m"
            ),
            equalTo(
                "FROM *:sample_data,sample_data, (TS *:k8s,k8s | STATS m=max(rate(network.total_bytes_in)) BY cluster) | KEEP cluster, m"
            )
        );
    }

    public void testConvertSubqueryToRemoteIndicesTsSubqueryQuotedIndex() {
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices(
                "FROM sample_data, (TS \"k8s-downsampled\" | WHERE @timestamp > \"2025-10-07\") | KEEP cluster"
            ),
            equalTo(
                "FROM *:sample_data,sample_data, (TS \"*:k8s-downsampled,k8s-downsampled\" | WHERE @timestamp > \"2025-10-07\") | KEEP cluster"
            )
        );
    }

    public void testConvertSubqueryToRemoteIndicesBothBranchesTs() {
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices(
                "FROM (TS k8s | STATS m=max(rate(network.total_bytes_in))),"
                    + " (TS \"k8s-downsampled\" | STATS m=max(rate(network.total_bytes_in)))"
                    + " | KEEP m"
            ),
            equalTo(
                "FROM (TS *:k8s,k8s | STATS m=max(rate(network.total_bytes_in))),"
                    + " (TS \"*:k8s-downsampled,k8s-downsampled\" | STATS m=max(rate(network.total_bytes_in)))"
                    + " | KEEP m"
            )
        );
    }

    public void testConvertSubqueryToRemoteIndicesWithSetStatement() {
        String in = """
            SET unmapped_fields="nullify";
            FROM k8s, (from many_numbers)
            | KEEP network.total_bytes_in
            | RENAME network.total_bytes_in as x, x as y
            | RENAME y as z
            | KEEP *
            | SORT z
            | LIMIT 1""";
        String out = "SET unmapped_fields=\"nullify\";\n"
            + "FROM *:k8s,k8s, (FROM *:many_numbers,many_numbers)"
            + " | KEEP network.total_bytes_in"
            + " | RENAME network.total_bytes_in as x, x as y"
            + " | RENAME y as z"
            + " | KEEP *"
            + " | SORT z"
            + " | LIMIT 1";
        assertThat(EsqlTestUtils.convertSubqueryToRemoteIndices(in), equalTo(out));
    }

    public void testConvertSubqueryToRemoteIndicesWithMultipleSetStatements() {
        String in = """
            SET a=b;
            SET c=d;
            FROM employees, (FROM employees_incompatible | KEEP emp_no)
            | SORT emp_no""";
        String out = "SET a=b;\nSET c=d;\n"
            + "FROM *:employees,employees, (FROM *:employees_incompatible,employees_incompatible | KEEP emp_no)"
            + " | SORT emp_no";
        assertThat(EsqlTestUtils.convertSubqueryToRemoteIndices(in), equalTo(out));
    }

    public void testConvertSubqueryToRemoteIndicesMixedFromAndTs() {
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices(
                "FROM sample_data, (TS k8s | WHERE @timestamp > \"2025-10-07\"),"
                    + " (TS \"k8s-downsampled\" | WHERE @timestamp > \"2025-10-07\")"
                    + " | KEEP cluster"
            ),
            equalTo(
                "FROM *:sample_data,sample_data, (TS *:k8s,k8s | WHERE @timestamp > \"2025-10-07\"),"
                    + " (TS \"*:k8s-downsampled,k8s-downsampled\" | WHERE @timestamp > \"2025-10-07\")"
                    + " | KEEP cluster"
            )
        );
    }
}
