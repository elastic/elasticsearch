/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.ClassRule;

import java.nio.file.Path;

public class EsqlSpecIT extends EsqlSpecTestCase {
    private static final Path CSV_DATA_PATH = CsvTestUtils.createCsvDataDirectory();

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(CSV_DATA_PATH, spec -> {
        spec.plugin("inference-service-test").settings(nodeSpec -> LOGGING_CLUSTER_SETTINGS);
    });

    @Override
    protected Path getCsvDataPath() {
        return CSV_DATA_PATH;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public EsqlSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase, String instructions) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    @Override
    protected boolean supportsSourceFieldMapping() {
        return false;
    }

    @Override
    protected String maybeRandomizeQuery(String query) {
        return randomlyNullify(query);
    }
}
