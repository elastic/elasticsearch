/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ApproximationVerifierTests extends ApproximationTestCase {

    public void testVerify_validQuery() {
        verify("FROM test | WHERE emp_no<99 | SORT last_name | MV_EXPAND salary | STATS COUNT() BY gender");
        verify("FROM test | EVAL x=1 | DROP emp_no | STATS sum=SUM(salary) BY x | CHANGE_POINT sum ON x");
        verify("FROM test | KEEP gender, emp_no | RENAME gender AS whatever | STATS MEDIAN(emp_no) | LIMIT 1000");
        verify("FROM test | EVAL blah=1 | GROK last_name \"%{IP:x}\" | SAMPLE 0.1 | STATS a=COUNT() | LIMIT 100 | SORT a");
        verify("ROW i=[1,2,3] | EVAL x=TO_STRING(i) | DISSECT x \"%{x}\" | STATS i=10*POW(PERCENTILE(i, 0.5), 2) | LIMIT 10");
        verify("FROM test | URI_PARTS parts = last_name | STATS scheme_count = COUNT() BY parts.scheme | LIMIT 10");
        verify("FROM test | REGISTERED_DOMAIN rd = last_name | STATS c = COUNT() BY rd.registered_domain | LIMIT 10");
    }

    public void testVerify_validQuery_oldDataNodes() {
        TransportVersion OLD_VERSION = TransportVersionUtils.randomVersion();
        verify("FROM test | WHERE emp_no<99 | SORT last_name | MV_EXPAND salary | STATS COUNT() BY gender", OLD_VERSION);
        verify("FROM test | EVAL x=1 | DROP emp_no | STATS sum=SUM(salary) BY x | CHANGE_POINT sum ON x", OLD_VERSION);
        verify("FROM test | KEEP gender, emp_no | RENAME gender AS whatever | STATS MEDIAN(emp_no) | LIMIT 1000", OLD_VERSION);
        verify("FROM test | EVAL blah=1 | GROK last_name \"%{IP:x}\" | SAMPLE 0.1 | STATS a=COUNT() | LIMIT 100 | SORT a", OLD_VERSION);
        verify("ROW i=[1,2,3] | EVAL x=TO_STRING(i) | DISSECT x \"%{x}\" | STATS i=10*POW(PERCENTILE(i, 0.5), 2) | LIMIT 10", OLD_VERSION);
        verify("FROM test | URI_PARTS parts = last_name | STATS scheme_count = COUNT() BY parts.scheme | LIMIT 10", OLD_VERSION);
        verify("FROM test | REGISTERED_DOMAIN rd = last_name | STATS c = COUNT() BY rd.registered_domain | LIMIT 10", OLD_VERSION);
    }

    public void testVerify_inlineStats() {
        assumeTrue("needs approximation inline stats", EsqlCapabilities.Cap.APPROXIMATION_INLINE_STATS_V2.isEnabled());
        verify("FROM test | INLINE STATS COUNT() BY last_name | LIMIT 10");
    }

    public void testVerify_lookupJoin() {
        assumeTrue("needs approximation lookup join", EsqlCapabilities.Cap.APPROXIMATION_LOOKUP_JOIN_V2.isEnabled());
        verify(
            "FROM test | LOOKUP JOIN test_lookup ON emp_no | STATS COUNT()",
            TransportVersionUtils.randomVersionSupporting(TransportVersion.fromName("esql_approximation_lookup_join"))
        );
    }

    public void testVerify_lookupJoin_oldDataNodes() {
        assumeTrue("needs approximation lookup join", EsqlCapabilities.Cap.APPROXIMATION_LOOKUP_JOIN_V2.isEnabled());
        assertError(
            "FROM test | LOOKUP JOIN test_lookup ON emp_no | STATS COUNT()",
            TransportVersionUtils.randomVersionNotSupporting(TransportVersion.fromName("esql_approximation_lookup_join")),
            equalTo(
                "line 1:13: approximation not supported: query with [LOOKUP JOIN test_lookup ON emp_no] cannot be approximated on all nodes"
            )
        );
    }

    public void testVerify_fork() {
        assumeTrue("needs approximation fork", EsqlCapabilities.Cap.APPROXIMATION_FORK.isEnabled());
        verify("FROM test | FORK (EVAL x=1 | STATS c = COUNT()) (EVAL y=1 | STATS c = COUNT())");
        verify("FROM test | FORK (EVAL x=1) (EVAL y=1) | STATS c = COUNT()");
        verify("FROM test | FORK (WHERE true) (STATS c = COUNT())");
        verify("FROM test | STATS COUNT() | FORK (WHERE true) (WHERE true)");
    }

    public void testVerify_nestedSubqueries() {
        assumeTrue("needs approximation fork", EsqlCapabilities.Cap.APPROXIMATION_FORK.isEnabled());
        // We need the plan before optimization here, because otherwise the verification exception
        // "Nested subqueries are not supported" is thrown at the end of logical optimization.
        LogicalPlan plan = EsqlTestUtils.analyzer().addDefaultIndex().query("FROM test, (FROM test, (FROM test)) | STATS COUNT()");
        VerificationException exception = assertThrows(
            VerificationException.class,
            () -> ApproximationVerifier.verifyPlanOrThrow(plan, TransportVersion.current())
        );
        assertThat(
            exception.getMessage(),
            equalTo("line 1:18: approximation not supported: query with multiple or nested forks or subqueries cannot be approximated")
        );
    }

    public void testVerify_validQuery_queryProperties() {
        assertThat(
            verify("FROM test | SORT last_name | RENAME gender AS whatever | EVAL blah=1 | STATS COUNT() BY emp_no"),
            equalTo(new ApproximationVerifier.QueryProperties(true, true, null))
        );
        assertThat(
            verify("FROM test | STATS COUNT() BY emp_no | WHERE emp_no > 10 | LIMIT 3 | MV_EXPAND emp_no"),
            equalTo(new ApproximationVerifier.QueryProperties(true, true, null))
        );
        assertThat(
            verify("FROM test | STATS COUNT() BY emp_no | WHERE emp_no > 10 | LIMIT 3 | MV_EXPAND emp_no"),
            equalTo(new ApproximationVerifier.QueryProperties(true, true, null))
        );
        assertThat(
            verify("FROM test | WHERE emp_no > 10 | STATS SUM(emp_no)"),
            equalTo(new ApproximationVerifier.QueryProperties(false, false, null))
        );
        assertThat(
            verify("FROM test | SAMPLE 0.3 | STATS COUNT() BY emp_no"),
            equalTo(new ApproximationVerifier.QueryProperties(true, false, null))
        );
        assertThat(
            verify("FROM test | WHERE emp_no > 10 | STATS SUM(emp_no)"),
            equalTo(new ApproximationVerifier.QueryProperties(false, false, null))
        );
        assertThat(
            verify("FROM test | SAMPLE 0.3 | STATS COUNT() BY emp_no"),
            equalTo(new ApproximationVerifier.QueryProperties(true, false, null))
        );
        assertThat(
            verify("FROM test | MV_EXPAND gender | STATS COUNT() BY emp_no"),
            equalTo(new ApproximationVerifier.QueryProperties(true, true, null))
        );
        assertThat(
            verify("FROM test | MV_EXPAND gender | WHERE emp_no < 3 | STATS COUNT()"),
            equalTo(new ApproximationVerifier.QueryProperties(false, false, null))
        );
        assertThat(
            verify("FROM test | MV_EXPAND gender | WHERE emp_no < 3 | STATS COUNT()"),
            equalTo(new ApproximationVerifier.QueryProperties(false, false, null))
        );
    }

    public void testVerify_validQuery_queryProperties_inlineStats() {
        assumeTrue("needs approximation inline stats", EsqlCapabilities.Cap.APPROXIMATION_INLINE_STATS_V2.isEnabled());
        assertThat(
            verify("FROM test | WHERE emp_no < 3 | INLINE STATS COUNT()"),
            equalTo(new ApproximationVerifier.QueryProperties(false, false, null))
        );
    }

    public void testVerify_validForkQuery_queryProperties() {
        assumeTrue("needs approximation fork", EsqlCapabilities.Cap.APPROXIMATION_FORK.isEnabled());
        assertThat(
            verify("FROM test | FORK (STATS COUNT()) | EVAL x=1"),
            equalTo(
                new ApproximationVerifier.QueryProperties(null, null, List.of(new ApproximationVerifier.QueryProperties(false, true, null)))
            )
        );
        assertThat(
            verify("FROM test | FORK (STATS COUNT() BY emp_no) | EVAL x=1"),
            equalTo(
                new ApproximationVerifier.QueryProperties(null, null, List.of(new ApproximationVerifier.QueryProperties(true, true, null)))
            )
        );
        assertThat(
            verify("FROM test | FORK (WHERE emp_no < 100 | STATS COUNT()) | EVAL x=1"),
            equalTo(
                new ApproximationVerifier.QueryProperties(
                    null,
                    null,
                    List.of(new ApproximationVerifier.QueryProperties(false, false, null))
                )
            )
        );
        assertThat(
            verify("FROM test | FORK (MV_EXPAND emp_no | STATS COUNT()) | EVAL x=1"),
            equalTo(
                new ApproximationVerifier.QueryProperties(null, null, List.of(new ApproximationVerifier.QueryProperties(false, true, null)))
            )
        );
        assertThat(
            verify("FROM test | FORK (WHERE emp_no<1 | STATS COUNT()) (EVAL x=1) (STATS SUM(emp_no) BY emp_no) (STATS COUNT())"),
            equalTo(
                new ApproximationVerifier.QueryProperties(
                    null,
                    null,
                    Arrays.asList(
                        new ApproximationVerifier.QueryProperties(false, false, null),
                        null,
                        new ApproximationVerifier.QueryProperties(true, true, null),
                        new ApproximationVerifier.QueryProperties(false, true, null)
                    )
                )
            )
        );
        assertThat(
            verify("FROM test | STATS COUNT() BY emp_no | FORK (WHERE emp_no < 10) (WHERE emp_no == 42)"),
            equalTo(
                new ApproximationVerifier.QueryProperties(
                    null,
                    null,
                    Arrays.asList(
                        new ApproximationVerifier.QueryProperties(true, true, null),
                        new ApproximationVerifier.QueryProperties(true, true, null)
                    )
                )
            )
        );
        assertThat(
            verify("FROM test | FORK (EVAL x=1) (WHERE true) | STATS COUNT() BY emp_no"),
            equalTo(new ApproximationVerifier.QueryProperties(true, false, null))
        );
        assertThat(
            verify("FROM test | FORK (EVAL x=1) (WHERE true) | STATS SUM(emp_no)"),
            equalTo(new ApproximationVerifier.QueryProperties(false, false, null))
        );
    }

    public void testVerify_exactlyOneStats() {
        assertError(
            "FROM test | EVAL x = 1 | SORT emp_no | LIMIT 100 | MV_EXPAND x",
            equalTo("line 1:1: approximation not supported: query must have [STATS] with aggregation function(s) that can be approximated")
        );
        assertError(
            "FROM test | STATS COUNT() BY emp_no | STATS COUNT()",
            equalTo("line 1:39: approximation not supported: query with chained [STATS] cannot be approximated")
        );
    }

    public void testVerify_exactlyOneStats_inlineStats() {
        assumeTrue("needs approximation inline stats", EsqlCapabilities.Cap.APPROXIMATION_INLINE_STATS_V2.isEnabled());

        assertError(
            "FROM test | INLINE STATS count=COUNT() BY emp_no | STATS AVG(count)",
            equalTo("line 1:52: approximation not supported: query with chained [STATS] cannot be approximated")
        );
        assertError(
            "FROM test | STATS COUNT() BY emp_no | INLINE STATS COUNT()",
            equalTo("line 1:39: approximation not supported: query with chained [STATS] cannot be approximated")
        );
        assertError(
            "FROM test | INLINE STATS count=COUNT() | INLINE STATS SUM(count)",
            equalTo("line 1:42: approximation not supported: query with chained [STATS] cannot be approximated")
        );
    }

    public void testVerify_noChainedStats_inlineStats() {
        assumeTrue("needs approximation inline stats", EsqlCapabilities.Cap.APPROXIMATION_INLINE_STATS_V2.isEnabled());
        assertError(
            "FROM test | INLINE STATS count=COUNT() BY emp_no | STATS AVG(count)",
            equalTo("line 1:52: approximation not supported: query with chained [STATS] cannot be approximated")
        );
        assertError(
            "FROM test | STATS COUNT() BY emp_no | INLINE STATS COUNT()",
            equalTo("line 1:39: approximation not supported: query with chained [STATS] cannot be approximated")
        );
        assertError(
            "FROM test | INLINE STATS count=COUNT() | INLINE STATS SUM(count)",
            equalTo("line 1:42: approximation not supported: query with chained [STATS] cannot be approximated")
        );
    }

    public void testVerify_noChainedStats_fork() {
        assumeTrue("needs approximation fork", EsqlCapabilities.Cap.APPROXIMATION_FORK.isEnabled());
        assertError(
            "FROM test | STATS COUNT() BY emp_no | FORK (EVAL x=1) (STATS COUNT())",
            equalTo("line 1:56: approximation not supported: query with chained [STATS] cannot be approximated")
        );
        assertError(
            "FROM test | FORK (EVAL x=1) (STATS COUNT() BY emp_no) | STATS COUNT()",
            equalTo("line 1:57: approximation not supported: query with chained [STATS] cannot be approximated")
        );
        assertError(
            "FROM test | STATS COUNT() BY emp_no | FORK (EVAL x=1) (WHERE true) | STATS COUNT()",
            equalTo("line 1:70: approximation not supported: query with chained [STATS] cannot be approximated")
        );
        assertError(
            "FROM test | FORK (EVAL x=1) (STATS COUNT_DISTINCT(emp_no) BY emp_no) | STATS COUNT()",
            equalTo("line 1:72: approximation not supported: query with chained [STATS] cannot be approximated")
        );
        assertError(
            "FROM test | FORK (EVAL x=1) (WHERE true) | STATS COUNT() BY emp_no | STATS COUNT()",
            equalTo("line 1:70: approximation not supported: query with chained [STATS] cannot be approximated")
        );
    }

    public void testVerify_noChainedStats_subquery() {
        assumeTrue("needs approximation fork", EsqlCapabilities.Cap.APPROXIMATION_FORK.isEnabled());
        assertError(
            "FROM test, (FROM test | STATS x = MAX(emp_no) BY gender) | STATS c = COUNT(*)",
            equalTo("line 1:60: approximation not supported: query with chained [STATS] cannot be approximated")
        );
        assertError(
            "FROM test, (FROM test | INLINE STATS x = MAX(emp_no) BY gender) | STATS c = COUNT(*)",
            equalTo("line 1:67: approximation not supported: query with chained [STATS] cannot be approximated")
        );
    }

    public void testVerify_incompatibleSourceCommand() {
        assertError(
            "SHOW INFO | STATS COUNT()",
            equalTo("line 1:1: approximation not supported: query with [SHOW INFO] cannot be approximated")
        );
        assertError(
            "TS k8s | STATS RATE(network.total_bytes_in)",
            equalTo("line 1:1: approximation not supported: query with [TS k8s] cannot be approximated")
        );
    }

    public void testVerify_incompatibleProcessingCommand() {
        String expectedMessage = EsqlCapabilities.Cap.APPROXIMATION_FORK.isEnabled()
            ? "line 1:87: approximation not supported: query with [FUSE] cannot be approximated"
            : "line 1:42: approximation not supported: query with [FORK (WHERE true) (WHERE true)] cannot be approximated";
        assertError("FROM test METADATA _id, _index, _score | FORK (WHERE true) (WHERE true) | LIMIT 100 | FUSE", equalTo(expectedMessage));
    }

    public void testVerify_incompatibleProcessingCommandBeforeStats() {
        assertError(
            "FROM test | LIMIT 1000 | STATS COUNT()",
            equalTo("line 1:13: approximation not supported: query with [LIMIT 1000] before [STATS] cannot be approximated")
        );
        assertError(
            "FROM test | CHANGE_POINT salary ON emp_no | EVAL x=1 | DROP emp_no | STATS SUM(salary) BY x",
            equalTo(
                "line 1:13: approximation not supported: query with [CHANGE_POINT salary ON emp_no] before [STATS] cannot be approximated"
            )
        );
    }

    public void testVerify_incompatibleAggregation() {
        assertError(
            "FROM test | SORT emp_no | STATS MIN(emp_no) | LIMIT 100",
            equalTo("line 1:33: approximation not supported: aggregation function [MIN(emp_no)] cannot be approximated")
        );
        assertError(
            "FROM test | STATS SUM(emp_no), VALUES(emp_no), TOP(emp_no, 2, \"ASC\"), COUNT()",
            equalTo("line 1:32: approximation not supported: aggregation function [VALUES(emp_no)] cannot be approximated")
        );
        assertError(
            "FROM test | STATS 5+10*POW(MAX(emp_no), 2) BY gender",
            equalTo("line 1:28: approximation not supported: aggregation function [MAX(emp_no)] cannot be approximated")
        );
        assertError(
            "ROW x=[1,2]::DENSE_VECTOR | STATS SUM(x)",
            equalTo("line 1:35: approximation not supported: aggregation function [SUM(x)] must return a numeric value; got [DENSE_VECTOR]")
        );
    }

    public void testVerify_forkInvalidBranches() {
        assumeTrue("needs approximation fork", EsqlCapabilities.Cap.APPROXIMATION_FORK.isEnabled());
        // One branch is approximable, so the query is fine.
        verify("FROM test | FORK (STATS MAX(emp_no)) (WHERE true) (STATS COUNT(emp_no))");
        // No branch is approximable, so throw an error.
        assertError(
            "FROM test | FORK (STATS MAX(emp_no)) (WHERE true) (STATS COUNT_DISTINCT(emp_no))",
            equalTo("line 1:25: approximation not supported: aggregation function [MAX(emp_no)] cannot be approximated")
        );
    }
}
