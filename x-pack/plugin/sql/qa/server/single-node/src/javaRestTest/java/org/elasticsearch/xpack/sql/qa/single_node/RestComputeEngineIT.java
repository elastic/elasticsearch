/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.single_node;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.xpack.sql.qa.rest.RemoteClusterAwareSqlRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

public class RestComputeEngineIT extends RemoteClusterAwareSqlRestTestCase {

    public void testBasicCompute() throws IOException {
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
        assertEquals("{\"errors\":false}", EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));

        Request computeRequest = new Request("POST", "/_compute");
        computeRequest.setJsonEntity("""
            {
              "plan" : {
                "aggregation" : {
                  "mode" : "FINAL",
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
        assertEquals("{\"pages\":1,\"rows\":1}", EntityUtils.toString(computeResponse.getEntity(), StandardCharsets.UTF_8));
    }
}
