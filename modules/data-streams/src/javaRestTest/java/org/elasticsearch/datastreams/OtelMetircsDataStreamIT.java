/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.script.field.WriteField;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class OtelMetircsDataStreamIT extends AbstractDataStreamIT {

    @Override
    protected String indexTemplateName() {
        return "logs-otel@template";
    }

    @SuppressWarnings("unchecked")
    public void testOtelMapping() throws Exception {
        String dataStream = "metrics-generic.otel-default";
        createDataStream(client, dataStream);

        bulk(client, dataStream, List.of("""
            { "create" : {"dynamic_templates": {"metrics.my.gauge": "gauge_long"} } }
            """, String.format(Locale.ROOT, """
              {
              "@timestamp": "%s",
              "data_stream": {
                "type": "metrics",
                "dataset": "generic.otel",
                "namespace": "default"
              },
              "resource": {
                "attributes": {
                  "service.name": "my-service"
                }
              },
              "attributes": {
                "foo": "bar",
                "numeric": 42,
                "host.ip": "127.0.0.1"
              },
              "unit": "{thingies}",
              "metrics": {
                "my.gauge": 42
              }
            }
            """, System.currentTimeMillis() + ".123456")));
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
        assertThat(getValueFromPath(properties, List.of("@timestamp", "type")), is("date_nanos"));
        assertThat(getValueFromPath(properties, List.of("scope", "properties", "name", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("metrics", "properties", "my.gauge", "type")), is("long"));
        assertThat(getValueFromPath(properties, List.of("metrics", "properties", "my.gauge", "time_series_metric")), is("gauge"));
        assertThat(getValueFromPath(properties, List.of("attributes", "properties", "numeric", "type")), is("long"));
        assertThat(getValueFromPath(properties, List.of("attributes", "properties", "host.ip", "type")), is("ip"));
        assertThat(getValueFromPath(properties, List.of("attributes", "properties", "foo", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("attributes", "properties", "foo", "ignore_above")), is(1024));
        assertThat(
            getValueFromPath(
                properties,
                List.of("resource", "properties", "attributes", "properties", "service.name", "time_series_dimension")
            ),
            is(true)
        );
    }
}
