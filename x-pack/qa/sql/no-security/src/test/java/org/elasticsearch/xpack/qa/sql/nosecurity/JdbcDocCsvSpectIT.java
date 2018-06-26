/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.nosecurity;

import org.elasticsearch.xpack.qa.sql.jdbc.CsvTestUtils.CsvTestCase;
import org.elasticsearch.xpack.qa.sql.jdbc.DocsCsvSpecTestCase;

public class JdbcDocCsvSpectIT extends DocsCsvSpecTestCase {

    public JdbcDocCsvSpectIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber, testCase);
    }
}
