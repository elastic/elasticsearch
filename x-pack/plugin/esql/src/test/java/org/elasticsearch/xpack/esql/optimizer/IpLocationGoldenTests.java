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
 * Golden tests for IP_LOCATION command optimizer behavior.
 */
public class IpLocationGoldenTests extends GoldenTestCase {

    /**
     * Filters on fields unrelated to IP_LOCATION output should be pushed below IP_LOCATION,
     * while filters on IP_LOCATION-derived fields (g.city_name) must stay above.
     */
    public void testPushDownFilterPastIpLocation() {
        assumeTrue("requires ip_location command capability", EsqlCapabilities.Cap.IP_LOCATION_COMMAND.isEnabled());
        String query = """
            FROM employees
            | WHERE emp_no > 10000
            | ip_location g = first_name
            | WHERE g.city_name == "London" AND salary > 5000
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOGICAL_OPTIMIZATION));
    }

    /**
     * IP_LOCATION should be pushed below a Project (KEEP) so the optimized tree is
     * Project -> IpLocation -> Limit -> EsRelation.
     */
    public void testPushDownIpLocationPastProject() {
        assumeTrue("requires ip_location command capability", EsqlCapabilities.Cap.IP_LOCATION_COMMAND.isEnabled());
        String query = """
            FROM employees
            | rename first_name as x
            | keep x
            | ip_location g = x
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOGICAL_OPTIMIZATION));
    }

    /**
     * Multiple SORT commands around IP_LOCATION should be combined into a single TopN,
     * keeping only the later sort order (g.city_name).
     */
    public void testCombineOrderByThroughIpLocation() {
        assumeTrue("requires ip_location command capability", EsqlCapabilities.Cap.IP_LOCATION_COMMAND.isEnabled());
        String query = """
            FROM employees
            | sort emp_no
            | ip_location g = first_name
            | sort g.city_name
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOGICAL_OPTIMIZATION));
    }

    /**
     * A filter on an IP_LOCATION-derived field (g.city_name) must NOT be pushed past IpLocation.
     */
    public void testFilterOnIpLocationIsNotPushedDown() {
        assumeTrue("requires ip_location command capability", EsqlCapabilities.Cap.IP_LOCATION_COMMAND.isEnabled());
        String query = """
            FROM employees
            | ip_location g = first_name
            | WHERE g.city_name == "London"
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOGICAL_OPTIMIZATION));
    }

    /**
     * When IP_LOCATION input is a constant_keyword field with a known value, a WHERE on that
     * same field should be folded away (not pushed into EsQueryExec).
     */
    public void testConstantFieldIpLocationFilter() {
        assumeTrue("requires ip_location command capability", EsqlCapabilities.Cap.IP_LOCATION_COMMAND.isEnabled());
        String query = """
            FROM all_types
            | ip_location g = `constant_keyword-foo`
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
