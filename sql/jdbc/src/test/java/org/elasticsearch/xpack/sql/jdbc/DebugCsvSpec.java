/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.sql.jdbc.framework.JdbcTestUtils;

import java.nio.file.Path;
import java.util.List;

@TestLogging(JdbcTestUtils.SQL_TRACE)
public class DebugCsvSpec extends CsvSpecIT {

    @ParametersFactory(shuffle = false, argumentFormatting = SqlSpecIT.PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        Parser parser = specParser();
        return readScriptSpec("/debug.csv-spec", parser);
    }

    public DebugCsvSpec(String groupName, String testName, Integer lineNumber, Path source, CsvTestCase testCase) {
        super(groupName, testName, lineNumber, source, testCase);
    }

    @Override
    protected boolean logEsResultSet() {
        return true;
    }
}