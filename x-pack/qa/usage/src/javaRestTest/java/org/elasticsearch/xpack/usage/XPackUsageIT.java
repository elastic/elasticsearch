/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.usage;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/*
 * This class is meant to test the _xpack/usage API. That API pulls in code from various plugins so it cannot be integration tested
 * within x-pack/plugin/core.
 */
public class XPackUsageIT extends ESRestTestCase {
    static final String BASIC_AUTH_VALUE = basicAuthHeaderValue("rest_user", new SecureString("rest-user-password".toCharArray()));

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    /*
     * This method checks the data_lifecyle portion of the usage API response.
     */
    public void testDataLifecycle() throws Exception {
        // Make sure that everything is zero when there are no datastreams:
        assertDataLifecycleUsageResults(0, 0, 0, 0.0);
        createDataStream("ds1", TimeValue.timeValueMillis(10000));
        assertDataLifecycleUsageResults(1, 10000, 10000, 10000.0);
        createDataStream("ds2", TimeValue.timeValueMillis(5000));
        assertDataLifecycleUsageResults(2, 5000, 10000, 7500.0);
        // Make sure the counters don't change if we add a datastream that has no lifecycle:
        createDataStreamNoLifecycle("ds3");
        assertDataLifecycleUsageResults(2, 5000, 10000, 7500.0);
        // Make sure that deleting a datastream takes it out of the numbers:
        deleteDataStream("ds1");
        assertDataLifecycleUsageResults(1, 5000, 5000, 5000.0);
    }

    @SuppressWarnings("unchecked")
    private void assertDataLifecycleUsageResults(int count, int minimumRetention, int maximumRetention, double averageRetention)
        throws Exception {
        Response response = client().performRequest(new Request("GET", "/_xpack/usage"));
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        Map<String, ?> responseMap = entityAsMap(response);
        Map<String, ?> dataLifecycleMap = (Map<String, ?>) responseMap.get("data_lifecycle");
        assertThat(dataLifecycleMap.get("available"), equalTo(true));
        assertThat(dataLifecycleMap.get("enabled"), equalTo(true));
        assertThat(dataLifecycleMap.get("count"), equalTo(count));
        assertThat(dataLifecycleMap.get("rollover_config"), notNullValue());
        Map<String, Object> retentionMap = (Map<String, Object>) dataLifecycleMap.get("retention");
        assertThat(retentionMap.size(), equalTo(3));
        assertThat(retentionMap.get("minimum_millis"), equalTo(minimumRetention));
        assertThat(retentionMap.get("maximum_millis"), equalTo(maximumRetention));
        assertThat(retentionMap.get("average_millis"), equalTo(averageRetention));
    }

    private void createDataStream(String name, TimeValue retention) throws Exception {
        String lifecycle = Strings.format("""
            ,
                    "lifecycle": {
                        "data_retention":"%s"
                    }
            """, retention);
        this.createDataStream(name, lifecycle);
    }

    private void createDataStreamNoLifecycle(String name) throws Exception {
        this.createDataStream(name, "");
    }

    private void createDataStream(String name, String lifecycle) throws Exception {
        Request componentTemplateRequest = new Request("PUT", "_component_template/mappings_" + name);
        componentTemplateRequest.setJsonEntity(Strings.format("""
                    {
                      "template": {
                        "mappings": {
                          "properties": {
                            "@timestamp": {
                              "type": "date",
                              "format": "date_optional_time||epoch_millis"
                            },
                            "message": {
                              "type": "wildcard"
                            }
                          }
                        }%s
                      }
                    }
            """, lifecycle));
        client().performRequest(componentTemplateRequest);
        Request indexTemplateRequest = new Request("PUT", "_index_template/index-template-" + name);
        indexTemplateRequest.setJsonEntity(Strings.format("""
            {
              "index_patterns": ["%s*"],
              "data_stream": { },
              "composed_of": [ "mappings_%s" ]
            }""", name, name));
        client().performRequest(indexTemplateRequest);
        Request bulkRequest = new Request("PUT", name + "/_bulk");
        bulkRequest.setJsonEntity("""
            { "create":{ } }
            { "@timestamp": "2099-05-06T16:21:15.000Z", "message": "192.0.2.42 - - [06/May/2099:16:21:15 +0000] message1" }
            { "create":{ } }
            { "@timestamp": "2099-05-06T16:25:42.000Z", "message": "192.0.2.255 - - [06/May/2099:16:25:42 +0000] message2" }
            """);
        client().performRequest(bulkRequest);
    }

    private void deleteDataStream(String name) throws Exception {
        client().performRequest(new Request("DELETE", "_data_stream/" + name));
    }
}
