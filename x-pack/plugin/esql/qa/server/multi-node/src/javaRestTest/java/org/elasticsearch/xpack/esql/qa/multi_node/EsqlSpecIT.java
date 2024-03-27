/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.elasticsearch.xpack.ql.CsvSpecReader.CsvTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EsqlSpecIT extends EsqlSpecTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public EsqlSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase, Mode mode) {
        super(fileName, groupName, testName, lineNumber, testCase, mode);
    }

    @Override
    protected Map<String, Object> runEsql(RestEsqlTestCase.RequestObjectBuilder requestObject, List<String> expectedWarnings)
        throws IOException {
        return super.runEsql(requestObject.esqlVersion("snapshot"), expectedWarnings);
    }
}
