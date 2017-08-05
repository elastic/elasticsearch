/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.sql.jdbc.framework.JdbcTestUtils;

import java.nio.file.Path;
import java.util.List;

public abstract class DebugCsvSpec extends CsvSpecIT {

    @ParametersFactory(shuffle = false, argumentFormatting = SqlSpecIT.PARAM_FORMATTING) // NOCOMMIT are we sure?!
    public static List<Object[]> readScriptSpec() throws Exception {
        JdbcTestUtils.sqlLogging();

        CsvSpecParser parser = new CsvSpecParser();
        return readScriptSpec("/debug.csv-spec", parser);
    }

    public DebugCsvSpec(String groupName, String testName, Integer lineNumber, Path source, CsvTestCase testCase) {
        super(groupName, testName, lineNumber, source, testCase);
    }

    //    @Override
    //    public void assertResults(ResultSet expected, ResultSet actual) throws SQLException {
    //        Logger logger = Loggers.getLogger("org.elasticsearch.xpack.sql.test");
    //        Loggers.setLevel(logger, "INFO");
    //
    //        JdbcTestUtils.resultSetToLogger(logger, actual);
    //    }
}