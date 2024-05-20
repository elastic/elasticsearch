/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class DatafeedWithoutSecurityRestIT extends ESRestTestCase {

    @Before
    public void setUpData() throws Exception {
        addAirlineData();
    }

    /**
     * The main purpose this test is to ensure the X-elastic-product
     * header is returned when security is disabled. The vast majority
     * of Datafeed test coverage is in DatafeedJobsRestIT but that
     * suite runs with security enabled.
     */
    public void testPreviewMissingHeader() throws Exception {
        String jobId = "missing-header";
        Request createJobRequest = new Request("PUT", "/_ml/anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "description": "Aggs job",
              "analysis_config": {
                "bucket_span": "1h",
                "detectors": [
                  {
                    "function": "count",
                    "partition_field_name": "airline"
                  }
                ],
                "influencers": [
                  "airline"
                ],
                "model_prune_window": "30d"
              },
                  "model_plot_config": {
                    "enabled": false,
                    "annotations_enabled": false
                  },
                  "analysis_limits": {
                    "model_memory_limit": "11mb",
                    "categorization_examples_limit": 4
                  },
              "data_description" : {"time_field": "time stamp"}
            }""");
        client().performRequest(createJobRequest);

        String datafeedId = "datafeed-" + jobId;
        Request createDatafeedRequest = new Request("PUT", "/_ml/datafeeds/" + datafeedId);
        createDatafeedRequest.setJsonEntity("""
            {
                "job_id": "missing-header",
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match_all": {}
                            }
                        ]
                    }
                },
                "indices": [
                    "airline-data"
                ],
                "scroll_size": 1000
            }
            """);
        client().performRequest(createDatafeedRequest);

        Request getFeed = new Request("GET", "/_ml/datafeeds/" + datafeedId + "/_preview");
        RequestOptions.Builder options = getFeed.getOptions().toBuilder();
        getFeed.setOptions(options);
        var previewResponse = client().performRequest(getFeed);
        assertXProductResponseHeader(previewResponse);

        client().performRequest(new Request("POST", "/_ml/anomaly_detectors/" + jobId + "/_open"));
        Request startRequest = new Request("POST", "/_ml/datafeeds/" + datafeedId + "/_start");
        Response startDatafeedResponse = client().performRequest(startRequest);
        assertXProductResponseHeader(startDatafeedResponse);
    }

    private void assertXProductResponseHeader(Response response) {
        assertEquals("Elasticsearch", response.getHeader("X-elastic-product"));
    }

    private void addAirlineData() throws IOException {
        StringBuilder bulk = new StringBuilder();

        // Create index with source = enabled, doc_values = enabled, stored = false + multi-field
        Request createAirlineDataRequest = new Request("PUT", "/airline-data");
        // space in 'time stamp' is intentional
        createAirlineDataRequest.setJsonEntity("""
            {
              "mappings": {
                "runtime": {
                  "airline_lowercase_rt": {
                    "type": "keyword",
                    "script": {
                      "source": "emit(params._source.airline.toLowerCase())"
                    }
                  }
                },
                "properties": {
                  "time stamp": {
                    "type": "date"
                  },
                  "airline": {
                    "type": "text",
                    "fields": {
                      "text": {
                        "type": "text"
                      },
                      "keyword": {
                        "type": "keyword"
                      }
                    }
                  },
                  "responsetime": {
                    "type": "float"
                  }
                }
              }
            }""");
        client().performRequest(createAirlineDataRequest);

        bulk.append("""
            {"index": {"_index": "airline-data", "_id": 1}}
            {"time stamp":"2016-06-01T00:00:00Z","airline":"AAA","responsetime":135.22}
            {"index": {"_index": "airline-data", "_id": 2}}
            {"time stamp":"2016-06-01T01:59:00Z","airline":"AAA","responsetime":541.76}
            """);

        bulkIndex(bulk.toString());
    }

    private void bulkIndex(String bulk) throws IOException {
        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.addParameter("pretty", null);
        String bulkResponse = EntityUtils.toString(client().performRequest(bulkRequest).getEntity());
        assertThat(bulkResponse, not(containsString("\"errors\": false")));
    }

}
