/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.sql.qa.jdbc.CsvSpecTestCase;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.ql.CsvSpecReader.CsvTestCase;
import static org.elasticsearch.xpack.ql.CsvSpecReader.specParser;

public class JdbcFrozenCsvSpecIT extends CsvSpecTestCase {

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        return readScriptSpec("/slow/frozen.csv-spec", specParser());
    }

    @Override
    protected Properties connectionProperties() {
        Properties props = new Properties(super.connectionProperties());
        String timeout = String.valueOf(TimeUnit.MINUTES.toMillis(5));
        props.setProperty("connect.timeout", timeout);
        props.setProperty("network.timeout", timeout);
        props.setProperty("query.timeout", timeout);
        props.setProperty("page.timeout", timeout);

        return props;
    }

    public JdbcFrozenCsvSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber, testCase);
    }
}
