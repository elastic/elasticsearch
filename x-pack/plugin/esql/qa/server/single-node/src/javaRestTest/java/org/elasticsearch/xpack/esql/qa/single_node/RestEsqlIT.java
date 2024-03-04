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
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class RestEsqlIT extends RestEsqlTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(
        specBuilder -> specBuilder.plugin("mapper-size").plugin("mapper-murmur3")
    );

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

    public void testIncompatibleMappingsErrors() throws IOException {
        // create first index
        Request request = new Request("PUT", "/index1");
        request.setJsonEntity("""
            {
               "mappings": {
                 "_size": {
                   "enabled": true
                 },
                 "properties": {
                   "message": {
                     "type": "keyword",
                     "fields": {
                       "hash": {
                         "type": "murmur3"
                       }
                     }
                   }
                 }
               }
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        // create second index
        request = new Request("PUT", "/index2");
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "message": {
                    "type": "long",
                    "fields": {
                      "hash": {
                        "type": "integer"
                      }
                    }
                  }
                }
              }
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        // create alias
        request = new Request("POST", "/_aliases");
        request.setJsonEntity("""
            {
              "actions": [
                {
                  "add": {
                    "index": "index1",
                    "alias": "test_alias"
                  }
                },
                {
                  "add": {
                    "index": "index2",
                    "alias": "test_alias"
                  }
                }
              ]
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
        assertException(
            "from index1,index2 | stats count(message)",
            "VerificationException",
            "Cannot use field [message] due to ambiguities",
            "incompatible types: [keyword] in [index1], [long] in [index2]"
        );
        assertException(
            "from test_alias | where message is not null",
            "VerificationException",
            "Cannot use field [message] due to ambiguities",
            "incompatible types: [keyword] in [index1], [long] in [index2]"
        );
        assertException("from test_alias | where _size is not null | limit 1", "Unknown column [_size]");
        assertException(
            "from test_alias | where message.hash is not null | limit 1",
            "Cannot use field [message.hash] with unsupported type [murmur3]"
        );
        assertException(
            "from index1 | where message.hash is not null | limit 1",
            "Cannot use field [message.hash] with unsupported type [murmur3]"
        );
        // clean up
        assertThat(deleteIndex("index1").isAcknowledged(), Matchers.is(true));
        assertThat(deleteIndex("index2").isAcknowledged(), Matchers.is(true));
    }

    private void assertException(String query, String... errorMessages) throws IOException {
        ResponseException re = expectThrows(ResponseException.class, () -> runEsqlSync(new RequestObjectBuilder().query(query)));
        assertThat(re.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        for (var error : errorMessages) {
            assertThat(re.getMessage(), containsString(error));
        }
    }
}
