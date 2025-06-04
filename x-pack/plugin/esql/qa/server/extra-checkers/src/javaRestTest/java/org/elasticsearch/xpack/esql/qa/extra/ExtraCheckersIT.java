/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.extra;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import javax.swing.text.html.parser.Entity;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
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

    public void testWithCategorize() {
        ResponseException e = expectThrows(ResponseException.class, () -> runEsql("""
            {
              "query": "ROW message=\\"foo bar\\" | STATS COUNT(*) BY CATEGORIZE(message) | LIMIT 1"
            }"""));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("line 1:43: CATEGORIZE is unsupported"));
    }

    public void testWithoutCategorize() throws IOException {
        String result = runEsql("""
            {
              "query": "ROW message=\\"foo bar\\" | STATS COUNT(*) | LIMIT 1"
            }""");
        assertThat(result, containsString("""
              "columns" : [
                {
                  "name" : "COUNT(*)",
                  "type" : "long"
                }
              ],
              "values" : [
                [
                  1
                ]
              ]
            """));
    }

    private String runEsql(String json) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(json);
        request.addParameter("error_trace", "");
        request.addParameter("pretty", "");
        Response response = client().performRequest(request);
        return EntityUtils.toString(response.getEntity());
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
