/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.Assert;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class RestEsqlIT extends RestEsqlTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory
    public static List<Object[]> modes() {
        return Arrays.stream(Mode.values()).map(m -> new Object[] { m }).toList();
    }

    public RestEsqlIT(Mode mode) {
        super(mode);
    }

    public void testBasicEsql() throws IOException {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            b.append(String.format(Locale.ROOT, """
                {"create":{"_index":"%s"}}
                {"@timestamp":"2020-12-12","test":"value%s","value":%d}
                """, testIndexName(), i, i));
        }
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.addParameter("filter_path", "errors");
        bulk.setJsonEntity(b.toString());
        Response response = client().performRequest(bulk);
        Assert.assertEquals("{\"errors\":false}", EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));

        RequestObjectBuilder builder = new RequestObjectBuilder().query(fromIndex() + " | stats avg(value)");
        if (Build.current().isSnapshot()) {
            builder.pragmas(Settings.builder().put("data_partitioning", "shard").build());
        }
        Map<String, Object> result = runEsql(builder);
        assertEquals(2, result.size());
        Map<String, String> colA = Map.of("name", "avg(value)", "type", "double");
        assertEquals(List.of(colA), result.get("columns"));
        assertEquals(List.of(List.of(499.5d)), result.get("values"));
    }

    public void testInvalidPragma() throws IOException {
        assumeTrue("pragma only enabled on snapshot builds", Build.current().isSnapshot());
        createIndex("test-index");
        for (int i = 0; i < 10; i++) {
            Request request = new Request("POST", "/test-index/_doc/");
            request.addParameter("refresh", "true");
            request.setJsonEntity("{\"f\":" + i + "}");
            assertOK(client().performRequest(request));
        }
        RequestObjectBuilder builder = new RequestObjectBuilder().query("from test-index | limit 1 | keep f");
        builder.pragmas(Settings.builder().put("data_partitioning", "invalid-option").build());
        ResponseException re = expectThrows(ResponseException.class, () -> runEsqlSync(builder));
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("No enum constant"));

        assertThat(deleteIndex("test-index").isAcknowledged(), is(true)); // clean up
    }

    public void testPragmaNotAllowed() throws IOException {
        assumeFalse("pragma only disabled on release builds", Build.current().isSnapshot());
        RequestObjectBuilder builder = new RequestObjectBuilder().query("row a = 1, b = 2");
        builder.pragmas(Settings.builder().put("data_partitioning", "shard").build());
        ResponseException re = expectThrows(ResponseException.class, () -> runEsqlSync(builder));
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("[pragma] only allowed in snapshot builds"));
    }
}
