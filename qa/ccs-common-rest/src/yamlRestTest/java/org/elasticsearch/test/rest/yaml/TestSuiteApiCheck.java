/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml;

/**
 * Contains the logic for which test suites should execute which tests.
 * YAML tests are split between multiple suites to avoid the test suite timing out.
 */
class TestSuiteApiCheck {

    /**
     * Returns true if the test suite should run the tests for the given API.
     * @param testSuite a concrete subclass of ESClientYamlSuiteTestCase
     * @param apiName The API name as described in the rest spec e.g. `search`
     * @return True if the test
     */
    public static boolean shouldExecuteTest(ESClientYamlSuiteTestCase testSuite, String apiName) {
        return switch (testSuite) {
            case CssSearchYamlTestSuiteIT cssSearch -> isSearchApi(apiName);
            case RcsCcsSearchYamlTestSuiteIT rssSearch -> isSearchApi(apiName);
            default -> true;
        };
    }

    private static boolean isSearchApi(String apiName) {
        return apiName.startsWith("search") || apiName.startsWith("msearch");
    }

    private TestSuiteApiCheck() {
    }
}
