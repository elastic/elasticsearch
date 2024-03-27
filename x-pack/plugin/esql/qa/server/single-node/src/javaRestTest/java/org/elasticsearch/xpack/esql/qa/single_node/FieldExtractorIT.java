/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.FieldExtractorTestCase;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FieldExtractorIT extends FieldExtractorTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Map<String, Object> runEsqlSync(RestEsqlTestCase.RequestObjectBuilder requestObject) throws IOException {
        return super.runEsqlSync(requestObject.esqlVersion("snapshot"));
    }
}
