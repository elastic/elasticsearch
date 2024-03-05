/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.script.field.WriteField;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.datastreams.LogsDataStreamIT.bulk;
import static org.elasticsearch.datastreams.LogsDataStreamIT.createDataStream;
import static org.elasticsearch.datastreams.LogsDataStreamIT.getMappingProperties;
import static org.elasticsearch.datastreams.LogsDataStreamIT.getValueFromPath;
import static org.elasticsearch.datastreams.LogsDataStreamIT.getWriteBackingIndex;
import static org.elasticsearch.datastreams.LogsDataStreamIT.search;
import static org.elasticsearch.datastreams.LogsDataStreamIT.waitForIndexTemplate;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class OtelMetircsDataStreamIT extends DisabledSecurityDataStreamTestCase {

    private RestClient client;

    @Before
    public void setup() throws Exception {
        client = client();
        waitForIndexTemplate(client, "logs-otel@template");
    }

    @After
    public void cleanUp() throws IOException {
        adminClient().performRequest(new Request("DELETE", "_data_stream/*"));
    }

    @SuppressWarnings("unchecked")
    public void testOtelMapping() throws Exception {
        String dataStream = "metrics-otel.generic-default";
        createDataStream(client, dataStream);

        bulk(client, dataStream, List.of("""
            { "create" : {"dynamic_templates": {"metrics.my.gauge": "gauge"} } }
            """, """
              {
              "@timestamp": "%s",
              "data_stream": {
                "type": "metrics",
                "dataset": "otel.generic",
                "namespace": "default"
              },
              "resource": {
                "attributes": {
                  "service.name": "my-service"
                }
              },
              "attributes": {
                "foo": "bar"
              },
              "metrics": {
                "my.gauge": 42
              }
            }
            """.formatted(System.currentTimeMillis() + ".123456")));
        Map<String, Object> response = search(client, dataStream, """
            {
              "query": {
                "exists": {
                  "field": "my.gauge"
                }
              },
              "size": 0,
              "aggs": {
                "avg_value": {
                  "avg": {
                    "field": "my.gauge"
                  }
                }
              }
            }
            """);

        assertThat(new WriteField("aggregations.avg_value.value", () -> response).get(-1), equalTo(42.0));

        Map<String, Object> properties = getMappingProperties(client, getWriteBackingIndex(client, dataStream));
        assertThat(getValueFromPath(properties, List.of("metrics", "properties", "my.gauge", "type")), is("long"));
        assertThat(getValueFromPath(properties, List.of("metrics", "properties", "my.gauge", "time_series_metric")), is("gauge"));
    }
}
