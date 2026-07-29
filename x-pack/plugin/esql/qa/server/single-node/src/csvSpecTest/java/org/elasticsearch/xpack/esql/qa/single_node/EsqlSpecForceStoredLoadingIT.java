/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;

import java.util.List;

/**
 * Runs all csv-spec tests with {@code STORED} field-extract preference. Tests tagged
 * {@code requestStored: SKIP} are filtered out by
 * {@link AbstractEsqlSpecForceStoredLoadingIT#csvSpecParameters()}.
 */
public class EsqlSpecForceStoredLoadingIT extends AbstractEsqlSpecForceStoredLoadingIT {

    public EsqlSpecForceStoredLoadingIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s", shuffle = false)
    public static List<Object[]> readScriptSpec() throws Exception {
        return csvSpecParameters();
    }
}
