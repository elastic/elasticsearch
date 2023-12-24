/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import org.elasticsearch.Version;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.elasticsearch.xpack.ql.CsvSpecReader.CsvTestCase;

import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;

public class MixedClusterEsqlSpecIT extends EsqlSpecTestCase {

    static final Version bwcVersion = Version.fromString(System.getProperty("tests.bwc_nodes_version"));
    static final Version newVersion = Version.fromString(System.getProperty("tests.new_nodes_version"));

    public MixedClusterEsqlSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber, testCase);
    }

    @Override
    protected void shouldSkipTest(String testName) {
        assumeTrue("Test " + testName + " is skipped on " + bwcVersion, isEnabled(testName, bwcVersion));
        assumeTrue("Test " + testName + " is skipped on " + newVersion, isEnabled(testName, newVersion));
    }
}
