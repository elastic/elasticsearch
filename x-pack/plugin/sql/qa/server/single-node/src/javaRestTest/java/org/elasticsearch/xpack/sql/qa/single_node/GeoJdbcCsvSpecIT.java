/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.sql.qa.geo.GeoCsvSpecTestCase;
import org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.CsvTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.specParser;

public class GeoJdbcCsvSpecIT extends GeoCsvSpecTestCase {

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        List<Object[]> list = new ArrayList<>();
        list.addAll(GeoCsvSpecTestCase.readScriptSpec());
        list.addAll(readScriptSpec("/single-node-only/command-sys-geo.csv-spec", specParser()));
        return list;
    }

    public GeoJdbcCsvSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber, testCase);
    }
}
