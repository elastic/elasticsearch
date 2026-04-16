/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute.FieldName;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.EnumSet;

/**
 * Golden tests for USER_AGENT command optimizer behavior.
 * Consolidates USER_AGENT-related optimizer tests that were previously spread across
 * {@link LogicalPlanOptimizerTests}, {@link PhysicalPlanOptimizerTests},
 * {@link LocalPhysicalPlanOptimizerTests}, and
 * {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineFiltersTests}.
 */
public class UserAgentGoldenTests extends GoldenTestCase {

    /**
     * Filters on fields unrelated to USER_AGENT output should be pushed below USER_AGENT,
     * while filters on UA-derived fields (ua.name) must stay above.
     */
    public void testPushDownFilterPastUserAgent() {
        assumeTrue("requires user_agent command capability", EsqlCapabilities.Cap.USER_AGENT_COMMAND.isEnabled());
        String query = """
            FROM employees
            | WHERE emp_no > 10000
            | user_agent ua = first_name WITH { "extract_device_type": true }
            | WHERE ua.name == "Chrome" AND salary > 5000
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOGICAL_OPTIMIZATION));
    }

    /**
     * USER_AGENT should be pushed below a Project (KEEP) so the optimized tree is
     * Project -> UserAgent -> Limit -> EsRelation.
     */
    public void testPushDownUserAgentPastProject() {
        assumeTrue("requires user_agent command capability", EsqlCapabilities.Cap.USER_AGENT_COMMAND.isEnabled());
        String query = """
            FROM employees
            | rename first_name as x
            | keep x
            | user_agent ua = x WITH { "extract_device_type": true }
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOGICAL_OPTIMIZATION));
    }

    /**
     * Multiple SORT commands around USER_AGENT should be combined into a single TopN,
     * keeping only the later sort order (ua.name).
     */
    public void testCombineOrderByThroughUserAgent() {
        assumeTrue("requires user_agent command capability", EsqlCapabilities.Cap.USER_AGENT_COMMAND.isEnabled());
        String query = """
            FROM employees
            | sort emp_no
            | user_agent ua = first_name WITH { "extract_device_type": true }
            | sort ua.name
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOGICAL_OPTIMIZATION));
    }

    /**
     * A filter on a USER_AGENT-derived field (ua.name) must NOT be pushed past UserAgent.
     */
    public void testFilterOnUserAgentIsNotPushedDown() {
        assumeTrue("requires user_agent command capability", EsqlCapabilities.Cap.USER_AGENT_COMMAND.isEnabled());
        String query = """
            FROM employees
            | user_agent ua = first_name WITH { "extract_device_type": true }
            | WHERE ua.name == "Chrome"
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOGICAL_OPTIMIZATION));
    }

    /**
     * When USER_AGENT input is a constant_keyword field with a known value, a WHERE on that
     * same field should be folded away (not pushed into EsQueryExec).
     */
    public void testConstantFieldUserAgentFilter() {
        assumeTrue("requires user_agent command capability", EsqlCapabilities.Cap.USER_AGENT_COMMAND.isEnabled());
        String query = """
            FROM all_types
            | user_agent ua = `constant_keyword-foo` WITH { "extract_device_type": true }
            | WHERE `constant_keyword-foo` == "foo"
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL_OPTIMIZATION), CONSTANT_KEYWORD_STATS);
    }

    private static final SearchStats CONSTANT_KEYWORD_STATS = new EsqlTestUtils.TestSearchStats() {
        @Override
        public boolean isSingleValue(FieldName field) {
            return true;
        }

        @Override
        public String constantValue(FieldName name) {
            return name.string().startsWith("constant_keyword") ? "foo" : null;
        }
    };
}
