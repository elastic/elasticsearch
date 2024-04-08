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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class DatafeedJobsRestIT extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE_SUPER_USER = UsernamePasswordToken.basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );
    private static final String BASIC_AUTH_VALUE_ML_ADMIN = UsernamePasswordToken.basicAuthHeaderValue(
        "ml_admin",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );
    private static final String BASIC_AUTH_VALUE_ML_ADMIN_WITH_SOME_DATA_ACCESS = UsernamePasswordToken.basicAuthHeaderValue(
        "ml_admin_plus_data",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE_SUPER_USER).build();
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    private static void setupDataAccessRole(String index) throws IOException {
        Request request = new Request("PUT", "/_security/role/test_data_access");
        request.setJsonEntity(Strings.format("""
            {  "indices" : [    { "names": ["%s"], "privileges": ["read"] }  ]}
            """, index));
        client().performRequest(request);
    }

    private void setupFullAccessRole(String index) throws IOException {
        Request request = new Request("PUT", "/_security/role/test_data_access");
        request.setJsonEntity(Strings.format("""
            {  "indices" : [    { "names": ["%s"], "privileges": ["all"] }  ]}
            """, index));
        client().performRequest(request);
    }

    private void setupUser(String user, List<String> roles) throws IOException {
        String password = new String(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING.getChars());

        Request request = new Request("PUT", "/_security/user/" + user);
        request.setJsonEntity(Strings.format("""
            { "password" : "%s", "roles" : [ %s ]}
            """, password, roles.stream().map(unquoted -> "\"" + unquoted + "\"").collect(Collectors.joining(", "))));
        client().performRequest(request);
    }

    @Before
    public void setUpData() throws Exception {
        setupDataAccessRole("network-data");
        // This user has admin rights on machine learning, but (importantly for the tests) no rights
        // on any of the data indexes
        setupUser("ml_admin", Collections.singletonList("machine_learning_admin"));
        // This user has admin rights on machine learning, and read access to the network-data index
        setupUser("ml_admin_plus_data", Arrays.asList("machine_learning_admin", "test_data_access"));
        addAirlineData();
        addNetworkData("network-data");
    }

    private void addAirlineData() throws IOException {
        StringBuilder bulk = new StringBuilder();

        Request createEmptyAirlineDataRequest = new Request("PUT", "/airline-data-empty");
        // space in 'time stamp' is intentional
        createEmptyAirlineDataRequest.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "time stamp": {
                    "type": "date"
                  },
                  "airline": {
                    "type": "keyword"
                  },
                  "responsetime": {
                    "type": "float"
                  }
                }
              }
            }""");
        client().performRequest(createEmptyAirlineDataRequest);

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

        // Create index with source = enabled, doc_values = disabled (except time), stored = false
        Request createAirlineDataDisabledDocValues = new Request("PUT", "/airline-data-disabled-doc-values");
        createAirlineDataDisabledDocValues.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "time stamp": {
                    "type": "date"
                  },
                  "airline": {
                    "type": "keyword",
                    "doc_values": false
                  },
                  "responsetime": {
                    "type": "float",
                    "doc_values": false
                  }
                }
              }
            }""");
        client().performRequest(createAirlineDataDisabledDocValues);

        bulk.append("""
            {"index": {"_index": "airline-data-disabled-doc-values", "_id": 1}}
            {"time stamp":"2016-06-01T00:00:00Z","airline":"AAA","responsetime":135.22}
            {"index": {"_index": "airline-data-disabled-doc-values", "_id": 2}}
            {"time stamp":"2016-06-01T01:59:00Z","airline":"AAA","responsetime":541.76}
            """);

        // Create index with source = disabled, doc_values = enabled (except time), stored = true
        Request createAirlineDataDisabledSource = new Request("PUT", "/airline-data-disabled-source");
        createAirlineDataDisabledSource.setJsonEntity("""
            {
              "mappings": {
                "_source": {
                  "enabled": false
                },
                "properties": {
                  "time stamp": {
                    "type": "date",
                    "store": true
                  },
                  "airline": {
                    "type": "keyword",
                    "store": true
                  },
                  "responsetime": {
                    "type": "float",
                    "store": true
                  }
                }
              }
            }""");

        bulk.append("""
            {"index": {"_index": "airline-data-disabled-source", "_id": 1}}
            {"time stamp":"2016-06-01T00:00:00Z","airline":"AAA","responsetime":135.22}
            {"index": {"_index": "airline-data-disabled-source", "_id": 2}}
            {"time stamp":"2016-06-01T01:59:00Z","airline":"AAA","responsetime":541.76}
            """);

        // Create index with nested documents
        Request createAirlineDataNested = new Request("PUT", "/nested-data");
        createAirlineDataNested.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "time": {
                    "type": "date"
                  }
                }
              }
            }""");
        client().performRequest(createAirlineDataNested);

        bulk.append("""
            {"index": {"_index": "nested-data", "_id": 1}}
            {"time":"2016-06-01T00:00:00Z", "responsetime":{"millis":135.22}, "airline":[{"name": "foo"}]}
            {"index": {"_index": "nested-data", "_id": 2}}
            {"time":"2016-06-01T01:59:00Z", "responsetime":{"millis":222.00}, "airline":[{"name": "bar"}]}
            """);

        // Create index with multiple docs per time interval for aggregation testing
        Request createAirlineDataAggs = new Request("PUT", "/airline-data-aggs");
        // space in 'time stamp' is intentional
        createAirlineDataAggs.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "time stamp": {
                    "type": "date"
                  },
                  "airline": {
                    "type": "keyword"
                  },
                  "responsetime": {
                    "type": "float"
                  }
                }
              }
            }""");
        client().performRequest(createAirlineDataAggs);

        bulk.append("""
            {"index": {"_index": "airline-data-aggs", "_id": 1}}
            {"time stamp":"2016-06-01T00:00:00Z","airline":"AAA","responsetime":100.0}
            {"index": {"_index": "airline-data-aggs", "_id": 2}}
            {"time stamp":"2016-06-01T00:01:00Z","airline":"AAA","responsetime":200.0}
            {"index": {"_index": "airline-data-aggs", "_id": 3}}
            {"time stamp":"2016-06-01T00:00:00Z","airline":"BBB","responsetime":1000.0}
            {"index": {"_index": "airline-data-aggs", "_id": 4}}
            {"time stamp":"2016-06-01T00:01:00Z","airline":"BBB","responsetime":2000.0}
            {"index": {"_index": "airline-data-aggs", "_id": 5}}
            {"time stamp":"2016-06-01T01:00:00Z","airline":"AAA","responsetime":300.0}
            {"index": {"_index": "airline-data-aggs", "_id": 6}}
            {"time stamp":"2016-06-01T01:01:00Z","airline":"AAA","responsetime":400.0}
            {"index": {"_index": "airline-data-aggs", "_id": 7}}
            {"time stamp":"2016-06-01T01:00:00Z","airline":"BBB","responsetime":3000.0}
            {"index": {"_index": "airline-data-aggs", "_id": 8}}
            {"time stamp":"2016-06-01T01:01:00Z","airline":"BBB","responsetime":4000.0}
            """);

        bulkIndex(bulk.toString());
    }

    private void addNetworkData(String index) throws IOException {
        // Create index with source = enabled, doc_values = enabled, stored = false + multi-field
        Request createIndexRequest = new Request("PUT", index);
        createIndexRequest.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "timestamp": {
                    "type": "date"
                  },
                  "host": {
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
                  "network_bytes_out": {
                    "type": "long"
                  }
                }
              }
            }""");
        client().performRequest(createIndexRequest);

        StringBuilder bulk = new StringBuilder();
        String docTemplate = """
            {"timestamp":%d,"host":"%s","network_bytes_out":%d}""";
        Date date = new Date(1464739200735L);
        for (int i = 0; i < 120; i++) {
            long byteCount = randomNonNegativeLong();
            bulk.append("{\"index\": {\"_index\": \"").append(index).append("\"}}\n");
            bulk.append(Strings.format(docTemplate, date.getTime(), "hostA", byteCount)).append('\n');

            byteCount = randomNonNegativeLong();
            bulk.append("{\"index\": {\"_index\": \"").append(index).append("\"}}\n");
            bulk.append(Strings.format(docTemplate, date.getTime(), "hostB", byteCount)).append('\n');

            date = new Date(date.getTime() + 10_000);
        }

        bulkIndex(bulk.toString());
    }

    public void testLookbackOnlyWithMixedTypes() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-with-mixed-types", "airline-data").setShouldSucceedProcessing(true).execute();
    }

    public void testLookbackOnlyWithKeywordMultiField() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-with-keyword-multi-field", "airline-data").setAirlineVariant("airline.keyword")
            .setShouldSucceedProcessing(true)
            .execute();
    }

    public void testLookbackOnlyWithTextMultiField() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-with-keyword-multi-field", "airline-data").setAirlineVariant("airline.text")
            .setShouldSucceedProcessing(true)
            .execute();
    }

    public void testLookbackOnlyWithDocValuesDisabled() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-with-doc-values-disabled", "airline-data-disabled-doc-values").execute();
    }

    public void testLookbackOnlyWithSourceDisabled() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-with-source-disabled", "airline-data-disabled-source").execute();
    }

    public void testLookbackOnlyWithScriptFields() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-with-script-fields", "airline-data").setScriptedFields("""
            {"scripted_airline":{"script":{"lang":"painless","source":"doc['airline.keyword'].value"}}}""")
            .setAirlineVariant("scripted_airline")
            .execute();
    }

    public void testLookbackOnlyWithRuntimeFields() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-with-runtime-fields", "airline-data").setAirlineVariant("airline_lowercase_rt")
            .setShouldSucceedProcessing(true)
            .execute();
    }

    public void testLookbackonlyWithNestedFields() throws Exception {
        String jobId = "test-lookback-only-with-nested-fields";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "description": "Nested job",
              "analysis_config": {
                "bucket_span": "1h",
                "detectors": [
                  {
                    "function": "mean",
                    "field_name": "responsetime.millis",
                    "by_field_name": "airline.name"
                  }
                ]
              },  "data_description": {"time_field": "time"}
            }""");
        client().performRequest(createJobRequest);

        String datafeedId = jobId + "-datafeed";
        new DatafeedBuilder(datafeedId, jobId, "nested-data").build();
        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
        );
        String jobStatsResponseAsString = EntityUtils.toString(jobStatsResponse.getEntity());
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
    }

    public void testLookbackWithGeo() throws Exception {
        String jobId = "test-lookback-only-with-geo";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "description": "lat_long with geo_point",
              "analysis_config": {
                "bucket_span": "15m",
                "detectors": [
                  {
                    "function": "lat_long",
                    "field_name": "location"
                  }
                ]
              },  "data_description": {"time_field": "time"}
            }""");
        client().performRequest(createJobRequest);
        String datafeedId = jobId + "-datafeed";
        new DatafeedBuilder(datafeedId, jobId, "geo-data").build();

        StringBuilder bulk = new StringBuilder();

        Request createGeoData = new Request("PUT", "/geo-data");
        createGeoData.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "time": {
                    "type": "date"
                  },
                  "location": {
                    "type": "geo_point"
                  }
                }
              }
            }""");
        client().performRequest(createGeoData);

        bulk.append("""
            {"index": {"_index": "geo-data", "_id": 1}}
            {"time":"2016-06-01T00:00:00Z","location":{"lat":38.897676,"lon":-77.03653}}
            {"index": {"_index": "geo-data", "_id": 2}}
            {"time":"2016-06-01T00:05:00Z","location":{"lat":38.897676,"lon":-77.03653}}
            {"index": {"_index": "geo-data", "_id": 3}}
            {"time":"2016-06-01T00:10:00Z","location":{"lat":38.897676,"lon":-77.03653}}
            {"index": {"_index": "geo-data", "_id": 4}}
            {"time":"2016-06-01T00:15:00Z","location":{"lat":38.897676,"lon":-77.03653}}
            {"index": {"_index": "geo-data", "_id": 5}}
            {"time":"2016-06-01T00:20:00Z","location":{"lat":38.897676,"lon":-77.03653}}
            {"index": {"_index": "geo-data", "_id": 6}}
            {"time":"2016-06-01T00:25:00Z","location":{"lat":38.897676,"lon":-77.03653}}
            {"index": {"_index": "geo-data", "_id": 7}}
            {"time":"2016-06-01T00:30:00Z","location":{"lat":38.897676,"lon":-77.03653}}
            {"index": {"_index": "geo-data", "_id": 8}}
            {"time":"2016-06-01T00:40:00Z","location":{"lat":90.0,"lon":-77.03653}}
            {"index": {"_index": "geo-data", "_id": 9}}
            {"time":"2016-06-01T00:41:00Z","location":{"lat":38.897676,"lon":-77.03653}}
            """);
        bulkIndex(bulk.toString());

        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
        );
        String jobStatsResponseAsString = EntityUtils.toString(jobStatsResponse.getEntity());
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":9"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":9"));
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
    }

    public void testLookbackWithIndicesOptions() throws Exception {
        String jobId = "test-lookback-only-with-indices-options";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "description": "custom indices options",
              "analysis_config": {
                "bucket_span": "15m",
                "detectors": [
                  {
                    "function": "count"
                  }
                ]
              },  "data_description": {"time_field": "time"}
            }""");
        client().performRequest(createJobRequest);
        String datafeedId = jobId + "-datafeed";
        new DatafeedBuilder(datafeedId, jobId, "*hidden-*").setIndicesOptions("""
            {"expand_wildcards": ["all"],"allow_no_indices": true}""").build();

        StringBuilder bulk = new StringBuilder();

        Request createGeoData = new Request("PUT", "/.hidden-index");
        createGeoData.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "time": {
                    "type": "date"
                  },
                  "value": {
                    "type": "long"
                  }
                }
              },
              "settings": {
                "index.hidden": true
              }
            }""");
        client().performRequest(createGeoData);

        bulk.append("""
            {"index": {"_index": ".hidden-index", "_id": 1}}
            {"time":"2016-06-01T00:00:00Z","value": 1000}
            {"index": {"_index": ".hidden-index", "_id": 2}}
            {"time":"2016-06-01T00:05:00Z","value":1500}
            {"index": {"_index": ".hidden-index", "_id": 3}}
            {"time":"2016-06-01T00:10:00Z","value":1600}
            {"index": {"_index": ".hidden-index", "_id": 4}}
            {"time":"2016-06-01T00:15:00Z","value":100}
            {"index": {"_index": ".hidden-index", "_id": 5}}
            {"time":"2016-06-01T00:20:00Z","value":1}
            {"index": {"_index": ".hidden-index", "_id": 6}}
            {"time":"2016-06-01T00:25:00Z","value":1500}
            {"index": {"_index": ".hidden-index", "_id": 7}}
            {"time":"2016-06-01T00:30:00Z","value":1500}
            {"index": {"_index": ".hidden-index", "_id": 8}}
            {"time":"2016-06-01T00:40:00Z","value":2100}
            {"index": {"_index": ".hidden-index", "_id": 9}}
            {"time":"2016-06-01T00:41:00Z","value":0}
            """);
        bulkIndex(bulk.toString());

        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
        );
        String jobStatsResponseAsString = EntityUtils.toString(jobStatsResponse.getEntity());
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":9"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":9"));
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
    }

    public void testLookbackOnlyGivenEmptyIndex() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-given-empty-index", "airline-data-empty").setShouldSucceedInput(false)
            .setShouldSucceedProcessing(false)
            .execute();
    }

    public void testInsufficientSearchPrivilegesOnPut() throws Exception {
        String jobId = "privs-put-job";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "description": "Aggs job",
              "analysis_config": {
                "bucket_span": "1h",
                "summary_count_field_name": "doc_count",
                "detectors": [
                  {
                    "function": "mean",
                    "field_name": "responsetime",
                    "by_field_name":"airline"
                   }
                ]
              },
              "data_description" : {"time_field": "time stamp"}
            }""");
        client().performRequest(createJobRequest);

        String datafeedId = "datafeed-" + jobId;
        // This should be disallowed, because even though the ml_admin user has permission to
        // create a datafeed they DON'T have permission to search the index the datafeed is
        // configured to read
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> new DatafeedBuilder(datafeedId, jobId, "airline-data-aggs").setAuthHeader(BASIC_AUTH_VALUE_ML_ADMIN).build()
        );

        assertThat(e.getMessage(), containsString("Cannot create datafeed"));
        assertThat(e.getMessage(), containsString("user ml_admin lacks permissions on the indices"));
    }

    public void testInsufficientSearchPrivilegesOnPutWithJob() {
        String jobId = "privs-failed-put-job";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "description": "Aggs job",
              "datafeed_config": {"indexes": ["airline-data-aggs"]},
              "analysis_config": {
                "bucket_span": "1h",
                 "summary_count_field_name": "doc_count",
                "detectors": [
                  {
                    "function": "mean",
                    "field_name": "responsetime",
                    "by_field_name":"airline"
                   }
                ]
              },
              "data_description" : {"time_field": "time stamp"}
            }""");
        RequestOptions.Builder options = createJobRequest.getOptions().toBuilder();
        options.addHeader("Authorization", BASIC_AUTH_VALUE_ML_ADMIN);
        createJobRequest.setOptions(options);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(createJobRequest));
        assertThat(e.getMessage(), containsString("Cannot create datafeed"));
        assertThat(e.getMessage(), containsString("user ml_admin lacks permissions on the indices"));

        ResponseException missing = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId))
        );
        assertThat(missing.getMessage(), containsString("No known job with id"));
    }

    public void testCreationOnPutWithRollup() throws Exception {
        setupDataAccessRole("airline-data-aggs-rollup");
        String jobId = "privs-put-job-rollup";
        String datafeedId = "datafeed-" + jobId;
        final Response response = createJobAndDataFeed(jobId, datafeedId);

        assertEquals(200, response.getStatusLine().getStatusCode());
        assertThat(
            EntityUtils.toString(response.getEntity()),
            containsString("\"datafeed_id\":\"" + datafeedId + "\",\"job_id\":\"" + jobId + "\"")
        );
    }

    public void testInsufficientSearchPrivilegesOnPreview() throws Exception {
        String jobId = "privs-preview-job";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "description": "Aggs job",
              "analysis_config": {
                "bucket_span": "1h",
                "summary_count_field_name": "doc_count",
                "detectors": [
                  {
                    "function": "mean",
                    "field_name": "responsetime",
                    "by_field_name": "airline"
                  }
                ]
              },
              "data_description" : {"time_field": "time stamp"}
            }""");
        client().performRequest(createJobRequest);

        String datafeedId = "datafeed-" + jobId;
        new DatafeedBuilder(datafeedId, jobId, "airline-data-aggs").build();

        // This should be disallowed, because ml_admin is trying to preview a datafeed created by
        // by another user (x_pack_rest_user in this case) that will reveal the content of an index they
        // don't have permission to search directly
        Request getFeed = new Request("GET", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_preview");
        RequestOptions.Builder options = getFeed.getOptions().toBuilder();
        options.addHeader("Authorization", BASIC_AUTH_VALUE_ML_ADMIN);
        getFeed.setOptions(options);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(getFeed));
        assertThat(e.getMessage(), containsString("[indices:data/read/field_caps] is unauthorized for user [ml_admin]"));
    }

    public void testSecondaryAuthSearchPrivilegesLookBack() throws Exception {
        setupDataAccessRole("airline-data");
        String jobId = "secondary-privs-put-job";
        createJob(jobId, "airline.keyword");
        String datafeedId = "datafeed-" + jobId;
        // Primary auth header does not have access, but secondary auth does
        new DatafeedBuilder(datafeedId, jobId, "airline-data").setAuthHeader(BASIC_AUTH_VALUE_ML_ADMIN)
            .setSecondaryAuthHeader(BASIC_AUTH_VALUE_ML_ADMIN_WITH_SOME_DATA_ACCESS)
            .build();
        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);

        Response jobStatsResponse = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
        );
        String jobStatsResponseAsString = EntityUtils.toString(jobStatsResponse.getEntity());
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
    }

    public void testSecondaryAuthSearchPrivilegesOnPreview() throws Exception {
        setupDataAccessRole("airline-data");
        String jobId = "secondary-privs-preview-job";
        createJob(jobId, "airline.keyword");

        String datafeedId = "datafeed-" + jobId;
        new DatafeedBuilder(datafeedId, jobId, "airline-data").build();

        Request getFeed = new Request("GET", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_preview");
        RequestOptions.Builder options = getFeed.getOptions().toBuilder();
        options.addHeader("Authorization", BASIC_AUTH_VALUE_ML_ADMIN);
        options.addHeader("es-secondary-authorization", BASIC_AUTH_VALUE_ML_ADMIN_WITH_SOME_DATA_ACCESS);
        getFeed.setOptions(options);
        // Should not fail as secondary auth has permissions.
        var response = client().performRequest(getFeed);
        assertXProductResponseHeader(response);
    }

    private void assertXProductResponseHeader(Response response) {
        assertEquals("Elasticsearch", response.getHeader("X-elastic-product"));
    }

    public void testLookbackOnlyGivenAggregationsWithHistogram() throws Exception {
        String jobId = "aggs-histogram-job";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "description": "Aggs job",
              "analysis_config": {
                "bucket_span": "1h",
                "summary_count_field_name": "doc_count",
                "detectors": [
                  {
                    "function": "mean",
                    "field_name": "responsetime",
                    "by_field_name": "airline"
                  }
                ]
              },
              "data_description": {"time_field": "time stamp"}
            }""");
        client().performRequest(createJobRequest);

        String datafeedId = "datafeed-" + jobId;
        String aggregations = """
            {
              "buckets": {
                "histogram": {
                  "field": "time stamp",
                  "interval": 3600000
                },
                "aggregations": {
                  "time stamp": {
                    "max": {
                      "field": "time stamp"
                    }
                  },
                  "airline": {
                    "terms": {
                      "field": "airline",
                      "size": 10
                    },
                    "aggregations": {
                      "responsetime": {
                        "avg": {
                          "field": "responsetime"
                        }
                      }
                    }
                  }
                }
              }
            }""";
        new DatafeedBuilder(datafeedId, jobId, "airline-data-aggs").setAggregations(aggregations).build();
        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
        );
        String jobStatsResponseAsString = EntityUtils.toString(jobStatsResponse.getEntity());
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":4"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":4"));
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
    }

    /**
     * This test confirms the fix for <a href="https://github.com/elastic/elasticsearch/issues/104699">the issue
     * where a datafeed with aggregations that filter everything for a bucket can go into an infinite loop</a>.
     * In this test the filter in the aggregation is crazy as it literally filters everything. Real users would
     * have a filter that only occasionally results in no results from the aggregation while the query alone
     * returns data. But the code path that's exercised is the same.
     */
    public void testLookbackOnlyGivenAggregationsWithHistogramAndBucketFilter() throws Exception {
        String jobId = "aggs-histogram-filter-job";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "description": "Aggs job with dodgy filter",
              "analysis_config": {
                "bucket_span": "1h",
                "summary_count_field_name": "doc_count",
                "detectors": [
                  {
                    "function": "mean",
                    "field_name": "responsetime",
                    "by_field_name": "airline"
                  }
                ]
              },
              "data_description": {"time_field": "time stamp"}
            }""");
        client().performRequest(createJobRequest);

        String datafeedId = "datafeed-" + jobId;
        // The "filter_everything" aggregation in here means the output is always empty.
        String aggregations = """
            {
              "buckets": {
                "histogram": {
                  "field": "time stamp",
                  "interval": 3600000
                },
                "aggregations": {
                  "time stamp": {
                    "max": {
                      "field": "time stamp"
                    }
                  },
                  "filter_everything" : {
                    "filter": {
                      "term" : {
                        "airline": "does not exist"
                      }
                    },
                    "aggregations": {
                      "airline": {
                        "terms": {
                          "field": "airline",
                          "size": 10
                        },
                        "aggregations": {
                          "responsetime": {
                            "avg": {
                              "field": "responsetime"
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }""";
        // The chunking timespan of 1 hour here must be less than the span of the data in order for the problem to be reproduced
        new DatafeedBuilder(datafeedId, jobId, "airline-data-aggs").setChunkingTimespan("1h").setAggregations(aggregations).build();
        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
        );
        String jobStatsResponseAsString = EntityUtils.toString(jobStatsResponse.getEntity());
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":0"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":0"));
        assertThat(jobStatsResponseAsString, containsString("\"bucket_count\":0"));

        // The most important thing this test is asserting is that we don't go into an infinite loop!
    }

    public void testLookbackOnlyGivenAggregationsWithDateHistogram() throws Exception {
        String jobId = "aggs-date-histogram-job";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "description": "Aggs job",
              "analysis_config": {
                "bucket_span": "3600s",
                "summary_count_field_name": "doc_count",
                "detectors": [
                  {
                    "function": "mean",
                    "field_name": "responsetime",
                    "by_field_name": "airline"
                  }
                ]
              },
              "data_description": {"time_field": "time stamp"}
            }""");
        client().performRequest(createJobRequest);

        String datafeedId = "datafeed-" + jobId;
        String aggregations = """
            {
              "time stamp": {
                "date_histogram": {
                  "field": "time stamp",
                  "calendar_interval": "1h"
                },
                "aggregations": {
                  "time stamp": {
                    "max": {
                      "field": "time stamp"
                    }
                  },
                  "airline": {
                    "terms": {
                      "field": "airline",
                      "size": 10
                    },
                    "aggregations": {
                      "responsetime": {
                        "avg": {
                          "field": "responsetime"
                        }
                      }
                    }
                  }
                }
              }
            }""";
        new DatafeedBuilder(datafeedId, jobId, "airline-data-aggs").setAggregations(aggregations).build();
        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
        );
        String jobStatsResponseAsString = EntityUtils.toString(jobStatsResponse.getEntity());
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":4"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":4"));
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
    }

    public void testLookbackUsingDerivativeAggWithLargerHistogramBucketThanDataRate() throws Exception {
        String jobId = "derivative-agg-network-job";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "analysis_config": {
                "bucket_span": "300s",
                "summary_count_field_name": "doc_count",
                "detectors": [
                  {
                    "function": "mean",
                    "field_name": "bytes-delta",
                    "by_field_name": "hostname"
                  }
                ]
              },
              "data_description": {"time_field": "timestamp"}
            }""");
        client().performRequest(createJobRequest);

        String datafeedId = "datafeed-" + jobId;
        String aggregations = """
            {
              "hostname": {
                "terms": {
                  "field": "host.keyword",
                  "size": 10
                },
                "aggs": {
                  "buckets": {
                    "date_histogram": {
                      "field": "timestamp",
                      "fixed_interval": "60s"
                    },
                    "aggs": {
                      "timestamp": {
                        "max": {
                          "field": "timestamp"
                        }
                      },
                      "bytes-delta": {
                        "derivative": {
                          "buckets_path": "avg_bytes_out"
                        }
                      },
                      "avg_bytes_out": {
                        "avg": {
                          "field": "network_bytes_out"
                        }
                      }
                    }
                  }
                }
              }
            }""";
        new DatafeedBuilder(datafeedId, jobId, "network-data").setAggregations(aggregations).setChunkingTimespan("300s").build();

        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
        );
        String jobStatsResponseAsString = EntityUtils.toString(jobStatsResponse.getEntity());
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":40"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":40"));
        assertThat(jobStatsResponseAsString, containsString("\"out_of_order_timestamp_count\":0"));
        assertThat(jobStatsResponseAsString, containsString("\"bucket_count\":3"));
        // The derivative agg won't have values for the first bucket of each host
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":2"));
    }

    public void testLookbackUsingDerivativeAggWithSmallerHistogramBucketThanDataRate() throws Exception {
        String jobId = "derivative-agg-network-job";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "analysis_config": {
                "bucket_span": "300s",
                "summary_count_field_name": "doc_count",
                "detectors": [
                  {
                    "function": "mean",
                    "field_name": "bytes-delta",
                    "by_field_name": "hostname"
                  }
                ]
              },
              "data_description": {"time_field": "timestamp"}
            }""");
        client().performRequest(createJobRequest);

        String datafeedId = "datafeed-" + jobId;
        String aggregations = """
            {
              "hostname": {
                "terms": {
                  "field": "host.keyword",
                  "size": 10
                },
                "aggs": {
                  "buckets": {
                    "date_histogram": {
                      "field": "timestamp",
                      "fixed_interval": "5s"
                    },
                    "aggs": {
                      "timestamp": {
                        "max": {
                          "field": "timestamp"
                        }
                      },
                      "bytes-delta": {
                        "derivative": {
                          "buckets_path": "avg_bytes_out"
                        }
                      },
                      "avg_bytes_out": {
                        "avg": {
                          "field": "network_bytes_out"
                        }
                      }
                    }
                  }
                }
              }
            }""";
        new DatafeedBuilder(datafeedId, jobId, "network-data").setAggregations(aggregations).setChunkingTimespan("300s").build();

        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
        );
        String jobStatsResponseAsString = EntityUtils.toString(jobStatsResponse.getEntity());
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":240"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":240"));
    }

    public void testLookbackWithoutPermissions() throws Exception {
        String jobId = "permission-test-network-job";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "analysis_config": {
                "bucket_span": "300s",
                "summary_count_field_name": "doc_count",
                "detectors": [
                  {
                    "function": "mean",
                    "field_name": "bytes-delta",
                    "by_field_name": "hostname"
                  }
                ]
              },
              "data_description": {"time_field": "timestamp"}
            }""");
        client().performRequest(createJobRequest);

        String datafeedId = "datafeed-" + jobId;
        String aggregations = """
            {
              "hostname": {
                "terms": {
                  "field": "host.keyword",
                  "size": 10
                },
                "aggs": {
                  "buckets": {
                    "date_histogram": {
                      "field": "timestamp",
                      "fixed_interval": "5s"
                    },
                    "aggs": {
                      "timestamp": {
                        "max": {
                          "field": "timestamp"
                        }
                      },
                      "bytes-delta": {
                        "derivative": {
                          "buckets_path": "avg_bytes_out"
                        }
                      },
                      "avg_bytes_out": {
                        "avg": {
                          "field": "network_bytes_out"
                        }
                      }
                    }
                  }
                }
              }
            }""";

        // At the time we create the datafeed the user can access the network-data index that we have access to
        new DatafeedBuilder(datafeedId, jobId, "network-data").setAggregations(aggregations)
            .setChunkingTimespan("300s")
            .setAuthHeader(BASIC_AUTH_VALUE_ML_ADMIN_WITH_SOME_DATA_ACCESS)
            .build();

        // Change the role so that the user can no longer access network-data
        setupDataAccessRole("some-other-data");

        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId, BASIC_AUTH_VALUE_ML_ADMIN_WITH_SOME_DATA_ACCESS);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
        );
        String jobStatsResponseAsString = EntityUtils.toString(jobStatsResponse.getEntity());
        // We expect that no data made it through to the job
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":0"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":0"));

        // There should be a notification saying that there was a problem extracting data
        refreshAllIndices();
        Response notificationsResponse = client().performRequest(
            new Request("GET", NotificationsIndex.NOTIFICATIONS_INDEX + "/_search?size=1000&q=job_id:" + jobId)
        );
        String notificationsResponseAsString = EntityUtils.toString(notificationsResponse.getEntity());
        assertThat(
            notificationsResponseAsString,
            containsString(
                "\"message\":\"Datafeed is encountering errors extracting data: "
                    + "action [indices:data/read/search] is unauthorized"
                    + " for user [ml_admin_plus_data]"
                    + " with effective roles [machine_learning_admin,test_data_access]"
                    + " on indices [network-data]"
            )
        );
    }

    public void testLookbackWithPipelineBucketAgg() throws Exception {
        String jobId = "pipeline-bucket-agg-job";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "analysis_config": {
                "bucket_span": "1h",
                "summary_count_field_name": "doc_count",
                "detectors": [
                  {
                    "function": "mean",
                    "field_name": "percentile95_airlines_count"
                  }
                ]
              },
              "data_description": {"time_field": "time stamp"}
            }""");
        client().performRequest(createJobRequest);

        String datafeedId = "datafeed-" + jobId;
        String aggregations = """
            {
              "buckets": {
                "date_histogram": {
                  "field": "time stamp",
                  "fixed_interval": "15m"
                },
                "aggregations": {
                  "time stamp": {
                    "max": {
                      "field": "time stamp"
                    }
                  },
                  "airlines": {
                    "terms": {
                      "field": "airline.keyword",
                      "size": 10
                    }
                  },
                  "percentile95_airlines_count": {
                    "percentiles_bucket": {
                      "buckets_path": "airlines._count",
                      "percents": [ 95 ]
                    }
                  }
                }
              }
            }""";
        new DatafeedBuilder(datafeedId, jobId, "airline-data").setAggregations(aggregations).build();

        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
        );
        String jobStatsResponseAsString = EntityUtils.toString(jobStatsResponse.getEntity());
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"input_field_count\":4"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_field_count\":4"));
        assertThat(jobStatsResponseAsString, containsString("\"out_of_order_timestamp_count\":0"));
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
    }

    public void testLookbackOnlyGivenAggregationsWithHistogramAndRollupIndex() throws Exception {
        String jobId = "aggs-histogram-rollup-job";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "description": "Aggs job",
              "analysis_config": {
                "bucket_span": "1h",
                "summary_count_field_name": "doc_count",
                "detectors": [
                  {
                    "function": "mean",
                    "field_name": "responsetime",
                    "by_field_name": "airline"
                  }
                ]
              },
              "data_description": {"time_field": "time stamp"}
            }""");
        client().performRequest(createJobRequest);

        String rollupJobId = "rollup-" + jobId;
        Request createRollupRequest = new Request("PUT", "/_rollup/job/" + rollupJobId);
        createRollupRequest.setJsonEntity("""
            {
            "index_pattern": "airline-data-aggs",
                "rollup_index": "airline-data-aggs-rollup",
                "cron": "*/30 * * * * ?",
                "page_size" :1000,
                "groups" : {
                  "date_histogram": {
                    "field": "time stamp",
                    "fixed_interval": "2m",
                    "delay": "7d"
                  },
                  "terms": {
                    "fields": ["airline"]
                  }    },
                "metrics": [
                    {
                        "field": "responsetime",
                        "metrics": ["avg","min","max","sum"]
                    },
                    {
                        "field": "time stamp",
                        "metrics": ["min","max"]
                    }
                ]
            }""");
        client().performRequest(createRollupRequest);
        client().performRequest(new Request("POST", "/_rollup/job/" + rollupJobId + "/_start"));

        assertBusy(() -> {
            Response getRollup = client().performRequest(new Request("GET", "/_rollup/job/" + rollupJobId));
            String body = EntityUtils.toString(getRollup.getEntity());
            assertThat(body, containsString("\"job_state\":\"started\""));
            assertThat(body, containsString("\"rollups_indexed\":4"));
        }, 60, TimeUnit.SECONDS);

        client().performRequest(new Request("POST", "/_rollup/job/" + rollupJobId + "/_stop"));
        assertBusy(() -> {
            Response getRollup = client().performRequest(new Request("GET", "/_rollup/job/" + rollupJobId));
            assertThat(EntityUtils.toString(getRollup.getEntity()), containsString("\"job_state\":\"stopped\""));
        }, 60, TimeUnit.SECONDS);

        final Request refreshRollupIndex = new Request("POST", "airline-data-aggs-rollup/_refresh");
        client().performRequest(refreshRollupIndex);

        String datafeedId = "datafeed-" + jobId;
        String aggregations = """
            {
              "buckets": {
                "date_histogram": {
                  "field": "time stamp",
                  "fixed_interval": "3600000ms"
                },
                "aggregations": {
                  "time stamp": {
                    "max": {
                      "field": "time stamp"
                    }
                  },
                  "responsetime": {
                    "avg": {
                      "field": "responsetime"
                    }
                  }
                }
              }
            }""";
        new DatafeedBuilder(datafeedId, jobId, "airline-data-aggs-rollup").setAggregations(aggregations).build();
        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
        );
        String jobStatsResponseAsString = EntityUtils.toString(jobStatsResponse.getEntity());
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":2"));
    }

    public void testLookbackWithoutPermissionsAndRollup() throws Exception {
        setupFullAccessRole("airline-data-aggs-rollup");
        String jobId = "rollup-permission-test-network-job";
        String datafeedId = "datafeed-" + jobId;
        createJobAndDataFeed(jobId, datafeedId);

        // Change the role so that the user can no longer access network-data
        setupFullAccessRole("some-other-data");

        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId, BASIC_AUTH_VALUE_ML_ADMIN_WITH_SOME_DATA_ACCESS);
        waitUntilJobIsClosed(jobId);
        // There should be a notification saying that there was a problem extracting data
        refreshAllIndices();
        Response notificationsResponse = client().performRequest(
            new Request("GET", NotificationsIndex.NOTIFICATIONS_INDEX + "/_search?size=1000&q=job_id:" + jobId)
        );
        String notificationsResponseAsString = EntityUtils.toString(notificationsResponse.getEntity());
        assertThat(
            notificationsResponseAsString,
            containsString(
                "\"message\":\"Datafeed is encountering errors extracting data: "
                    + "action [indices:data/read/xpack/rollup/search] is unauthorized"
                    + " for user [ml_admin_plus_data]"
                    + " with effective roles [machine_learning_admin,test_data_access]"
                    + " on indices [airline-data-aggs-rollup]"
            )
        );
    }

    public void testLookbackWithSingleBucketAgg() throws Exception {
        String jobId = "aggs-date-histogram-with-single-bucket-agg-job";
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "description": "Aggs job",
              "analysis_config": {
                "bucket_span": "3600s",
                "summary_count_field_name": "doc_count",
                "detectors": [
                  {
                    "function": "mean",
                    "field_name": "responsetime"      }
                ]
              },
              "data_description": {"time_field": "time stamp"}
            }""");
        client().performRequest(createJobRequest);

        String datafeedId = "datafeed-" + jobId;
        String aggregations = """
            {
              "time stamp": {
                "date_histogram": {
                  "field": "time stamp",
                  "calendar_interval": "1h"
                },
                "aggregations": {
                  "time stamp": {
                    "max": {
                      "field": "time stamp"
                    }
                  },
                  "airlineFilter": {
                    "filter": {
                      "term": {
                        "airline": "AAA"
                      }
                    },
                    "aggregations": {
                      "responsetime": {
                        "avg": {
                          "field": "responsetime"
                        }
                      }
                    }
                  }
                }
              }
            }""";
        new DatafeedBuilder(datafeedId, jobId, "airline-data-aggs").setAggregations(aggregations).build();
        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
        );
        String jobStatsResponseAsString = EntityUtils.toString(jobStatsResponse.getEntity());
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
    }

    public void testRealtime() throws Exception {
        String jobId = "job-realtime-1";
        createJob(jobId, "airline");
        String datafeedId = jobId + "-datafeed";
        new DatafeedBuilder(datafeedId, jobId, "airline-data").setFrequency(TimeValue.timeValueSeconds(5)).build();
        openJob(client(), jobId);

        Request startRequest = new Request("POST", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_start");
        startRequest.addParameter("start", "2016-06-01T00:00:00Z");
        Response response = client().performRequest(startRequest);
        assertThat(EntityUtils.toString(response.getEntity()), containsString("\"started\":true"));

        // We should now be running in real time but may or may not have finished look back
        assertBusy(() -> {
            try {
                Response datafeedStatsResponse = client().performRequest(
                    new Request("GET", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_stats")
                );
                String body = EntityUtils.toString(datafeedStatsResponse.getEntity());
                assertThat(body, containsString("\"real_time_configured\":true"));
                assertThat(body, anyOf(containsString("\"real_time_running\":true"), containsString("\"real_time_running\":false")));
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
        });
        assertBusy(() -> {
            try {
                Response getJobResponse = client().performRequest(
                    new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
                );
                String responseAsString = EntityUtils.toString(getJobResponse.getEntity());
                assertThat(responseAsString, containsString("\"processed_record_count\":2"));
                assertThat(responseAsString, containsString("\"state\":\"opened\""));
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
        });

        // Model state should be persisted at the end of lookback
        // test a model snapshot is present
        assertBusy(() -> {
            try {
                Response getJobResponse = client().performRequest(
                    new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/model_snapshots")
                );
                String responseAsString = EntityUtils.toString(getJobResponse.getEntity());
                assertThat(responseAsString, containsString("\"count\":1"));
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
        });

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId))
        );
        response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(409));
        assertThat(
            EntityUtils.toString(response.getEntity()),
            containsString("Cannot delete job [" + jobId + "] because the job is opened")
        );

        // Look back should now be completed and we are still considered a real time datafeed (no endtime set)
        assertBusy(() -> {
            try {
                Response datafeedStatsResponse = client().performRequest(
                    new Request("GET", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_stats")
                );
                String body = EntityUtils.toString(datafeedStatsResponse.getEntity());
                assertThat(body, containsString("\"real_time_configured\":true"));
                assertThat(body, containsString("\"real_time_running\":true"));
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
        });

        response = client().performRequest(new Request("POST", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_stop"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(EntityUtils.toString(response.getEntity()), equalTo("{\"stopped\":true}"));

        client().performRequest(new Request("POST", "/_ml/anomaly_detectors/" + jobId + "/_close"));

        response = client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(EntityUtils.toString(response.getEntity()), equalTo("{\"acknowledged\":true}"));

        response = client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(EntityUtils.toString(response.getEntity()), equalTo("{\"acknowledged\":true}"));
    }

    public void testForceDeleteWhileDatafeedIsRunning() throws Exception {
        String jobId = "job-realtime-2";
        createJob(jobId, "airline");
        String datafeedId = jobId + "-datafeed";
        new DatafeedBuilder(datafeedId, jobId, "airline-data").build();
        openJob(client(), jobId);

        Request startRequest = new Request("POST", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_start");
        startRequest.addParameter("start", "2016-06-01T00:00:00Z");
        Response response = client().performRequest(startRequest);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(EntityUtils.toString(response.getEntity()), containsString("\"started\":true"));

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId))
        );
        response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(409));
        assertThat(
            EntityUtils.toString(response.getEntity()),
            containsString("Cannot delete datafeed [" + datafeedId + "] while its status is started")
        );

        Request forceDeleteRequest = new Request("DELETE", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId);
        forceDeleteRequest.addParameter("force", "true");
        response = client().performRequest(forceDeleteRequest);
        assertThat(EntityUtils.toString(response.getEntity()), equalTo("{\"acknowledged\":true}"));

        expectThrows(ResponseException.class, () -> client().performRequest(new Request("GET", "/_ml/datafeeds/" + datafeedId)));
    }

    private class LookbackOnlyTestHelper {
        private final String jobId;
        private final String dataIndex;
        private String airlineVariant;
        private String scriptedFields;
        private boolean shouldSucceedInput;
        private boolean shouldSucceedProcessing;

        LookbackOnlyTestHelper(String jobId, String dataIndex) {
            this.jobId = jobId;
            this.dataIndex = dataIndex;
            this.shouldSucceedInput = true;
            this.shouldSucceedProcessing = true;
            this.airlineVariant = "airline";
        }

        public LookbackOnlyTestHelper setScriptedFields(String scriptFields) {
            this.scriptedFields = scriptFields;
            return this;
        }

        public LookbackOnlyTestHelper setAirlineVariant(String airlineVariant) {
            this.airlineVariant = airlineVariant;
            return this;
        }

        public LookbackOnlyTestHelper setShouldSucceedInput(boolean value) {
            shouldSucceedInput = value;
            return this;
        }

        public LookbackOnlyTestHelper setShouldSucceedProcessing(boolean value) {
            shouldSucceedProcessing = value;
            return this;
        }

        public void execute() throws Exception {
            Response jobResponse = createJob(jobId, airlineVariant);
            assertXProductResponseHeader(jobResponse);
            assertThat(jobResponse.getStatusLine().getStatusCode(), equalTo(200));
            String datafeedId = "datafeed-" + jobId;
            new DatafeedBuilder(datafeedId, jobId, dataIndex).setScriptedFields(scriptedFields).build();
            openJob(client(), jobId);

            startDatafeedAndWaitUntilStopped(datafeedId);
            waitUntilJobIsClosed(jobId);

            Response jobStatsResponse = client().performRequest(
                new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
            );
            assertXProductResponseHeader(jobStatsResponse);
            String jobStatsResponseAsString = EntityUtils.toString(jobStatsResponse.getEntity());
            if (shouldSucceedInput) {
                assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":2"));
            } else {
                assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":0"));
            }
            if (shouldSucceedProcessing) {
                assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":2"));
            } else {
                assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":0"));
            }
            assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
        }
    }

    private void startDatafeedAndWaitUntilStopped(String datafeedId) throws Exception {
        startDatafeedAndWaitUntilStopped(datafeedId, BASIC_AUTH_VALUE_SUPER_USER);
    }

    private void startDatafeedAndWaitUntilStopped(String datafeedId, String authHeader) throws Exception {
        Request request = new Request("POST", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_start");
        request.addParameter("start", "2016-06-01T00:00:00Z");
        request.addParameter("end", "2016-06-02T00:00:00Z");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Authorization", authHeader);
        request.setOptions(options);
        Response startDatafeedResponse = client().performRequest(request);
        assertXProductResponseHeader(startDatafeedResponse);
        assertThat(EntityUtils.toString(startDatafeedResponse.getEntity()), containsString("\"started\":true"));
        assertBusy(() -> {
            try {
                Response datafeedStatsResponse = client().performRequest(
                    new Request("GET", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_stats")
                );
                assertXProductResponseHeader(datafeedStatsResponse);
                assertThat(EntityUtils.toString(datafeedStatsResponse.getEntity()), containsString("\"state\":\"stopped\""));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, 60, TimeUnit.SECONDS);
    }

    private void waitUntilJobIsClosed(String jobId) throws Exception {
        assertBusy(() -> {
            try {
                Response jobStatsResponse = client().performRequest(
                    new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats")
                );
                assertXProductResponseHeader(jobStatsResponse);
                assertThat(EntityUtils.toString(jobStatsResponse.getEntity()), containsString("\"state\":\"closed\""));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Response createJob(String id, String airlineVariant) throws Exception {
        Request request = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + id);
        request.setJsonEntity(Strings.format("""
            {
              "description": "Analysis of response time by airline",
              "analysis_config": {
                "bucket_span": "1h",
                "detectors": [ { "function": "mean", "field_name": "responsetime", "by_field_name": "%s" } ]
              },
              "data_description": {
                "format": "xcontent",
                "time_field": "time stamp",
                "time_format": "yyyy-MM-dd'T'HH:mm:ssX"
              }
            }""", airlineVariant));
        return client().performRequest(request);
    }

    public static void openJob(RestClient client, String jobId) throws IOException {
        client.performRequest(new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_open"));
    }

    @After
    public void clearMlState() throws Exception {
        new MlRestTestStateCleaner(logger, adminClient()).resetFeatures();
        // Don't check rollup jobs because we clear them in the superclass.
        waitForPendingTasks(adminClient(), taskName -> taskName.startsWith(RollupJob.NAME));
    }

    private static class DatafeedBuilder {
        String datafeedId;
        String jobId;
        String index;
        boolean source;
        String scriptedFields;
        String aggregations;
        String authHeader = BASIC_AUTH_VALUE_SUPER_USER;
        String secondaryAuthHeader = null;
        String chunkingTimespan;
        String indicesOptions;
        TimeValue frequency;

        DatafeedBuilder(String datafeedId, String jobId, String index) {
            this.datafeedId = datafeedId;
            this.jobId = jobId;
            this.index = index;
        }

        public DatafeedBuilder setFrequency(TimeValue frequency) {
            this.frequency = frequency;
            return this;
        }

        DatafeedBuilder setScriptedFields(String scriptedFields) {
            this.scriptedFields = scriptedFields;
            return this;
        }

        DatafeedBuilder setAggregations(String aggregations) {
            this.aggregations = aggregations;
            return this;
        }

        DatafeedBuilder setAuthHeader(String authHeader) {
            this.authHeader = authHeader;
            return this;
        }

        DatafeedBuilder setSecondaryAuthHeader(String authHeader) {
            this.secondaryAuthHeader = authHeader;
            return this;
        }

        DatafeedBuilder setChunkingTimespan(String timespan) {
            chunkingTimespan = timespan;
            return this;
        }

        DatafeedBuilder setIndicesOptions(String indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        Response build() throws IOException {
            Request request = new Request("PUT", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId);
            Object[] args = new Object[] {
                jobId,
                index,
                source ? ",\"_source\":true" : "",
                scriptedFields == null ? "" : ",\"script_fields\":" + scriptedFields,
                aggregations == null ? "" : ",\"aggs\":" + aggregations,
                frequency == null ? "" : ",\"frequency\":\"" + frequency + "\"",
                indicesOptions == null ? "" : ",\"indices_options\":" + indicesOptions,
                chunkingTimespan == null ? "" : Strings.format("""
                    ,"chunking_config":{"mode":"MANUAL","time_span":"%s"}
                    """, chunkingTimespan) };
            request.setJsonEntity(Strings.format("""
                {
                  "job_id": "%s",
                  "indexes":["%s"]
                   %s
                   %s
                   %s
                   %s
                   %s
                   %s
                }""", args));
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization", authHeader);
            if (this.secondaryAuthHeader != null) {
                options.addHeader("es-secondary-authorization", secondaryAuthHeader);
            }
            request.setOptions(options);
            return client().performRequest(request);
        }
    }

    private void bulkIndex(String bulk) throws IOException {
        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.addParameter("pretty", null);
        String bulkResponse = EntityUtils.toString(client().performRequest(bulkRequest).getEntity());
        assertThat(bulkResponse, not(containsString("\"errors\": false")));
    }

    private Response createJobAndDataFeed(String jobId, String datafeedId) throws IOException {
        Request createJobRequest = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        createJobRequest.setJsonEntity("""
            {
              "description": "Aggs job",
              "analysis_config": {
                "bucket_span": "1h",
                "summary_count_field_name": "doc_count",
                "detectors": [
                  {
                    "function": "mean",
                    "field_name": "responsetime",
                    "by_field_name": "airline"
                  }
                ]
              },
              "data_description": {"time_field": "time stamp"}
            }""");
        client().performRequest(createJobRequest);

        String rollupJobId = "rollup-" + jobId;
        Request createRollupRequest = new Request("PUT", "/_rollup/job/" + rollupJobId);
        createRollupRequest.setJsonEntity("""
            {
            "index_pattern": "airline-data-aggs",
                "rollup_index": "airline-data-aggs-rollup",
                "cron": "*/30 * * * * ?",
                "page_size" :1000,
                "groups" : {
                  "date_histogram": {
                    "field": "time stamp",
                    "fixed_interval": "2m",
                    "delay": "7d"
                  },
                  "terms": {
                    "fields": ["airline"]
                  }    },
                "metrics": [
                    {
                        "field": "responsetime",
                        "metrics": ["avg","min","max","sum"]
                    },
                    {
                        "field": "time stamp",
                        "metrics": ["min","max"]
                    }
                ]
            }""");
        client().performRequest(createRollupRequest);

        String aggregations = """
            {
              "buckets": {
                "date_histogram": {
                  "field": "time stamp",
                  "fixed_interval": "3600000ms"
                },
                "aggregations": {
                  "time stamp": {
                    "max": {
                      "field": "time stamp"
                    }
                  },
                  "responsetime": {
                    "avg": {
                      "field": "responsetime"
                    }
                  }
                }
              }
            }""";

        return new DatafeedBuilder(datafeedId, jobId, "airline-data-aggs-rollup").setAggregations(aggregations)
            .setAuthHeader(BASIC_AUTH_VALUE_ML_ADMIN_WITH_SOME_DATA_ACCESS)
            .build();
    }
}
