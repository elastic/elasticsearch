/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.sql.qa.jdbc.CsvSpecTestCase;
import org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.CsvTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.specParser;

public class JdbcCsvSpecIT extends CsvSpecTestCase {

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        List<Object[]> list = new ArrayList<>();
        list.addAll(CsvSpecTestCase.readScriptSpec());
        list.addAll(readScriptSpec("/single-node-only/command-sys.csv-spec", specParser()));
        return list;
    }

    public JdbcCsvSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber, testCase);
    }

    @Override
    protected int fetchSize() {
        // using a smaller fetchSize for nested documents' tests to uncover bugs
        // similar to https://github.com/elastic/elasticsearch/issues/35176 quicker
        return fileName.startsWith("nested") && randomBoolean() ? randomIntBetween(1, 5) : super.fetchSize();
    }
}
