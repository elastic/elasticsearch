/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.elasticsearch.xpack.ql.CsvSpecReader.CsvTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;

public class EsqlSpecIT extends EsqlSpecTestCase {
    public static ElasticsearchCluster cluster = Clusters.testCluster();
    public static ClosingTestRule<RestClient> client = new ClosingTestRule<>() {
        @Override
        protected RestClient provideObject() throws IOException {
            return startClient(cluster, Settings.builder().build());
        }
    };
    public static CsvLoader loader = new CsvLoader(client);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(cluster).around(client).around(loader);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public EsqlSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase, Mode mode) {
        super(fileName, groupName, testName, lineNumber, testCase, mode);
    }
}
