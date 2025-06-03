/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.extra;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class ExtraCheckersIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .shared(true)
        .plugin("extra-checkers")
        .build();

    public void testIncludesCategorize() throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("""
            {
              "query": "ROW message=\\"foo bar\\" | STATS COUNT(*) BY CATEGORIZE(message)"
            }""");
        request.addParameter("error_trace", "");
        Exception e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getMessage(), equalTo("ADFSADF"));
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
