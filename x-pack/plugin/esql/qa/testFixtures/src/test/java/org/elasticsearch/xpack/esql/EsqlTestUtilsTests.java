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

    public void testConvertSubqueryToRemoteIndicesBothClusterIndexIsRemoteOnly() {
        // `languages` is an enrich source index ingested into BOTH clusters, so its subquery source must be rewritten to the remote-only
        // `*:languages` to avoid double-counting rows, while the regular `sample_data` index uses `*:sample_data,sample_data`,
        // unmapped-load.loadUnmappedFieldTypeConflictAcrossSubqueriesIsUnsupported is an example of this pattern.
        String in = "FROM (FROM languages | EVAL x = 1), (FROM sample_data | EVAL x = 2) | KEEP x";
        String out = "FROM (FROM *:languages | EVAL x = 1), (FROM *:sample_data,sample_data | EVAL x = 2) | KEEP x";
        assertThat(EsqlTestUtils.convertSubqueryToRemoteIndices(in, Set.of("languages")), equalTo(out));
    }

    public void testConvertSubqueryToRemoteIndicesBothClusterIndexMixedInSameSubquery() {
        // A both-cluster index (`languages`) alongside a single-cluster index (`employees`) inside the same subquery FROM: only
        // `languages` becomes remote-only. unmapped-load.loadMultiIndexPartialMultiFieldInSubquery is an example of this pattern
        String in = "FROM (FROM employees, languages | WHERE emp_no == 10001) | KEEP emp_no";
        String out = "FROM (FROM *:employees,employees, *:languages | WHERE emp_no == 10001) | KEEP emp_no";
        assertThat(EsqlTestUtils.convertSubqueryToRemoteIndices(in, Set.of("languages")), equalTo(out));
    }

    public void testConvertWhereInSubqueryBothClusterIndexIsRemoteOnly() {
        // A both-cluster index inside a WHERE IN subquery body is also rewritten to remote-only.
        String in = "FROM sample_data | WHERE client_ip IN (FROM clientips | KEEP client_ip) | KEEP message";
        String out = "FROM *:sample_data,sample_data | WHERE client_ip IN (FROM *:clientips | KEEP client_ip) | KEEP message";
        assertThat(EsqlTestUtils.convertSubqueryToRemoteIndices(in, Set.of("clientips")), equalTo(out));
    }

    public void testConvertSubqueryToRemoteIndicesWithoutBothClusterSetIsUnchanged() {
        // Regression guard: with no both-cluster indices declared, every source keeps the *:index,index form.
        String in = "FROM (FROM languages | EVAL x = 1), (FROM sample_data | EVAL x = 2) | KEEP x";
        String out = "FROM (FROM *:languages,languages | EVAL x = 1), (FROM *:sample_data,sample_data | EVAL x = 2) | KEEP x";
        assertThat(EsqlTestUtils.convertSubqueryToRemoteIndices(in), equalTo(out));
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

    // ---- WHERE IN subquery rewriting ----

    public void testConvertWhereInSubqueryBasic() {
        // The IN subquery body's FROM is rewritten; the outer FROM is also rewritten.
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices(
                "FROM employees | WHERE emp_no IN (FROM employees | SORT emp_no ASC | LIMIT 3 | KEEP emp_no) | SORT emp_no | KEEP emp_no"
            ),
            equalTo(
                "FROM *:employees,employees"
                    + " | WHERE emp_no IN (FROM *:employees,employees | SORT emp_no ASC | LIMIT 3 | KEEP emp_no)"
                    + " | SORT emp_no | KEEP emp_no"
            )
        );
    }

    public void testConvertWhereNotInSubquery() {
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices(
                "FROM employees | WHERE emp_no NOT IN (FROM employees | SORT emp_no ASC | LIMIT 3 | KEEP emp_no) | SORT emp_no"
            ),
            equalTo(
                "FROM *:employees,employees"
                    + " | WHERE emp_no NOT IN (FROM *:employees,employees | SORT emp_no ASC | LIMIT 3 | KEEP emp_no)"
                    + " | SORT emp_no"
            )
        );
    }

    public void testConvertWhereInLiteralValueListUnchanged() {
        // Literal value lists must NOT be rewritten — they don't contain a source command.
        String query = "FROM employees | WHERE gender IN (\"F\", \"M\", null) | SORT emp_no";
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices(query),
            equalTo("FROM *:employees,employees | WHERE gender IN (\"F\", \"M\", null) | SORT emp_no")
        );
    }

    public void testConvertWhereInIntegerLiteralListUnchanged() {
        String query = "FROM employees | WHERE emp_no IN (10001, 10002, 10003) | SORT emp_no";
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices(query),
            equalTo("FROM *:employees,employees | WHERE emp_no IN (10001, 10002, 10003) | SORT emp_no")
        );
    }

    public void testConvertNestedInSubquery() {
        // Two levels of IN nesting: the inner IN body is inside the outer IN body.
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices(
                "FROM employees"
                    + " | WHERE emp_no IN ("
                    + "FROM employees"
                    + " | WHERE salary IN (FROM employees | SORT salary DESC | LIMIT 3 | KEEP salary)"
                    + " | KEEP emp_no"
                    + ")"
                    + " | SORT emp_no"
            ),
            equalTo(
                "FROM *:employees,employees"
                    + " | WHERE emp_no IN ("
                    + "FROM *:employees,employees"
                    + " | WHERE salary IN (FROM *:employees,employees | SORT salary DESC | LIMIT 3 | KEEP salary)"
                    + " | KEEP emp_no"
                    + ")"
                    + " | SORT emp_no"
            )
        );
    }

    public void testConvertFromUnionInsideInSubquery() {
        // FROM-union inside the IN subquery body: both union branches are rewritten.
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices(
                "FROM employees"
                    + " | WHERE emp_no IN (FROM (FROM employees | LIMIT 5), (FROM employees_incompatible | LIMIT 5) | KEEP emp_no)"
                    + " | SORT emp_no"
            ),
            equalTo(
                "FROM *:employees,employees"
                    + " | WHERE emp_no IN ("
                    + "FROM (FROM *:employees,employees | LIMIT 5), (FROM *:employees_incompatible,employees_incompatible | LIMIT 5)"
                    + " | KEEP emp_no)"
                    + " | SORT emp_no"
            )
        );
    }

    public void testConvertWhereInRowSubqueryUnchanged() {
        // ROW inside an IN subquery must be left untouched (no index to rewrite).
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices("FROM employees | WHERE emp_no IN (ROW emp_no = 10001) | SORT emp_no"),
            equalTo("FROM *:employees,employees | WHERE emp_no IN (ROW emp_no = 10001) | SORT emp_no")
        );
    }

    public void testConvertMultipleInSubqueriesInOneWhere() {
        // Two IN subqueries in a single WHERE clause — both must be rewritten.
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices(
                "FROM employees"
                    + " | WHERE emp_no NOT IN (FROM employees | LIMIT 3 | KEEP emp_no)"
                    + " AND emp_no IN (FROM employees | LIMIT 10 | KEEP emp_no)"
                    + " | SORT emp_no"
            ),
            equalTo(
                "FROM *:employees,employees"
                    + " | WHERE emp_no NOT IN (FROM *:employees,employees | LIMIT 3 | KEEP emp_no)"
                    + " AND emp_no IN (FROM *:employees,employees | LIMIT 10 | KEEP emp_no)"
                    + " | SORT emp_no"
            )
        );
    }

    public void testConvertInSubqueryInsideBooleanGrouping() {
        // The IN subquery is nested inside a parenthesised boolean expression.
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices(
                "FROM employees"
                    + " | WHERE (emp_no NOT IN (FROM employees | LIMIT 5 | KEEP emp_no) AND salary > 70000)"
                    + " OR emp_no IN (FROM employees | LIMIT 10 | KEEP emp_no)"
                    + " | SORT emp_no"
            ),
            equalTo(
                "FROM *:employees,employees"
                    + " | WHERE (emp_no NOT IN (FROM *:employees,employees | LIMIT 5 | KEEP emp_no) AND salary > 70000)"
                    + " OR emp_no IN (FROM *:employees,employees | LIMIT 10 | KEEP emp_no)"
                    + " | SORT emp_no"
            )
        );
    }

    public void testConvertInSubqueryAfterRowCommand() {
        // ROW + IN subquery
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices(
                "ROW emp_no = 10007" + " | WHERE emp_no IN (FROM employees | WHERE salary > 70000 | KEEP emp_no)"
            ),
            equalTo("ROW emp_no = 10007" + " | WHERE emp_no IN (FROM *:employees,employees | WHERE salary > 70000 | KEEP emp_no)")
        );
    }

    public void testConvertInSubqueryAfterTsCommand() {
        // TS + IN subquery
        assertThat(
            EsqlTestUtils.convertSubqueryToRemoteIndices(
                "TS employees | WHERE emp_no IN (FROM employees | SORT emp_no ASC | LIMIT 3 | KEEP emp_no) | SORT emp_no | KEEP emp_no"
            ),
            equalTo(
                "TS *:employees,employees"
                    + " | WHERE emp_no IN (FROM *:employees,employees | SORT emp_no ASC | LIMIT 3 | KEEP emp_no)"
                    + " | SORT emp_no | KEEP emp_no"
            )
        );
    }

    public void testConvertWhereInSubqueryMultiline() {
        // Multi-line formatting is handled: splitIgnoringParentheses joins the main pipe segments
        // with " | ", collapsing newlines in the FROM clause. The subquery body inside the IN (...)
        // parens is stripped of leading/trailing whitespace before recursion, so the surrounding
        // newlines inside the parens are dropped (the rewritten subquery is structurally equivalent).
        String in = """
            FROM employees
            | WHERE emp_no IN (
                FROM employees | SORT emp_no ASC | LIMIT 3 | KEEP emp_no
              )
            | SORT emp_no
            | KEEP emp_no""";
        String out = "FROM *:employees,employees"
            + " | WHERE emp_no IN (FROM *:employees,employees | SORT emp_no ASC | LIMIT 3 | KEEP emp_no)"
            + " | SORT emp_no"
            + " | KEEP emp_no";
        assertThat(EsqlTestUtils.convertSubqueryToRemoteIndices(in), equalTo(out));
    }

    public void testConvertWhereInSubqueryIdempotent() {
        // Calling the method twice on an already-converted query must return the query unchanged.
        String once = EsqlTestUtils.convertSubqueryToRemoteIndices(
            "FROM employees | WHERE emp_no IN (FROM employees | LIMIT 3 | KEEP emp_no) | SORT emp_no"
        );
        String twice = EsqlTestUtils.convertSubqueryToRemoteIndices(once);
        assertThat(twice, equalTo(once));
    }
}
