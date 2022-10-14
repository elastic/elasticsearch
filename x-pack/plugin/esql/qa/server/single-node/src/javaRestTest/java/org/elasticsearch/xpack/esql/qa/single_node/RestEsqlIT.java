/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.single_node;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class RestEsqlIT extends RestEsqlTestCase {

    @Experimental
    public void testComputeEndpoint() throws IOException {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            b.append(String.format(Locale.ROOT, """
                {"create":{"_index":"compute-index"}}
                {"@timestamp":"2020-12-12","test":"value%s","value":%d}
                """, i, i));
        }
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.addParameter("filter_path", "errors");
        bulk.setJsonEntity(b.toString());
        Response response = client().performRequest(bulk);
        Assert.assertEquals("{\"errors\":false}", EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));

        Request computeRequest = new Request("POST", "/_compute");
        computeRequest.setJsonEntity("""
            {
              "plan" : {
                "aggregation" : {
                  "mode" : "FINAL",
                  "groupBy" : { },
                  "aggs" : {
                    "value_avg" : {
                      "avg" : {
                        "field" : "value"
                      }
                    }
                  },
                  "source" : {
                    "aggregation" : {
                      "mode" : "PARTIAL",
                      "groupBy" : { },
                      "aggs" : {
                        "value_avg" : {
                          "avg" : {
                            "field" : "value"
                          }
                        }
                      },
                      "source" : {
                        "doc-values" : {
                          "field" : "value",
                          "source" : {
                            "lucene-source" : {
                              "indices" : "compute-index",
                              "query" : "*:*",
                              "parallelism" : "SINGLE"
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """);
        Response computeResponse = client().performRequest(computeRequest);
        Assert.assertThat(
            EntityUtils.toString(computeResponse.getEntity(), StandardCharsets.UTF_8),
            containsString("\"pages\":1,\"rows\":1")
        );
    }

    public void testBasicEsql() throws IOException {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            b.append(String.format(Locale.ROOT, """
                {"create":{"_index":"esql-index"}}
                {"@timestamp":"2020-12-12","test":"value%s","value":%d}
                """, i, i));
        }
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.addParameter("filter_path", "errors");
        bulk.setJsonEntity(b.toString());
        Response response = client().performRequest(bulk);
        Assert.assertEquals("{\"errors\":false}", EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));

        RequestObjectBuilder builder = new RequestObjectBuilder().query("from esql-index | stats avg(value)");
        if (Build.CURRENT.isSnapshot()) {
            builder.pragmas(Settings.builder().put("data_partitioning", "shard").build());
        }
        builder.build();
        Map<String, Object> result = runEsql(builder);
        assertEquals(2, result.size());
        Map<String, String> colA = Map.of("name", "avg(value)", "type", "double");
        assertEquals(List.of(colA), result.get("columns"));
        assertEquals(List.of(List.of(499.5d)), result.get("values"));
    }

    public void testInvalidPragma() throws IOException {
        assumeTrue("pragma only enabled on snapshot builds", Build.CURRENT.isSnapshot());
        RequestObjectBuilder builder = new RequestObjectBuilder().query("row a = 1, b = 2");
        builder.pragmas(Settings.builder().put("data_partitioning", "invalid-option").build());
        builder.build();
        ResponseException re = expectThrows(ResponseException.class, () -> runEsql(builder));
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("No enum constant"));
    }

    public void testPragmaNotAllowed() throws IOException {
        assumeFalse("pragma only disabled on release builds", Build.CURRENT.isSnapshot());
        RequestObjectBuilder builder = new RequestObjectBuilder().query("row a = 1, b = 2");
        builder.pragmas(Settings.builder().put("data_partitioning", "shard").build());
        builder.build();
        ResponseException re = expectThrows(ResponseException.class, () -> runEsql(builder));
        assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("[pragma] only allowed in snapshot builds"));
    }
}
