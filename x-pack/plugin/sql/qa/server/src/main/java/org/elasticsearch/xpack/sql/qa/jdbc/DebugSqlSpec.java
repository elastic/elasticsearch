/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.util.List;

public abstract class DebugSqlSpec extends SqlSpecTestCase {
    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        Parser parser = specParser();
        return readScriptSpec("/datetime.sql-spec", parser);
    }

    public DebugSqlSpec(String fileName, String groupName, String testName, Integer lineNumber, String query) {
        super(fileName, groupName, testName, lineNumber, query);
    }

    @Override
    protected boolean logEsResultSet() {
        return true;
    }
}
