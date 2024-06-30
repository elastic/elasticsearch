/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class OtelLogsDataStreamIT extends AbstractDataStreamIT {

    @Override
    protected String indexTemplateName() {
        return "logs-otel@template";
    }

    @SuppressWarnings("unchecked")
    public void testOtelMapping() throws Exception {
        String dataStream = "logs-generic.otel-default";
        createDataStream(client, dataStream);

        indexDoc(client, dataStream, """
            {
               "@timestamp": "1688394864123.456789",
               "body_text": "This is a log message",
               "data_stream": {
                 "type": "logs",
                 "dataset": "generic.otel",
                 "namespace": "default"
               },
               "dropped_attributes_count": 1,
               "severity_text": "Info",
               "severity_number": 9,
               "resource": {
                 "dropped_attributes_count": 1,
                 "attributes": {
                   "service.name": "my-service"
                 }
               },
               "scope": {
                 "dropped_attributes_count": 1,
                 "attributes": {
                   "scope-attr": "scope-attr-val-1"
                 }
               },
               "attributes": {
                 "foo.attr": "bar",
                 "complex.attribute": {
                   "foo": {
                     "bar": {
                       "baz": "qux"
                     }
                   }
                 }
               },
               "trace_flags": 1,
               "span_id": "0102040800000000",
               "trace_id": "08040201000000000000000000000000"
             }
            """);
        List<Object> hits = searchDocs(client, dataStream, """
            {
              "query": {
                "term": {
                  "foo.attr": "bar"
                }
              },
              "fields": [
                "*"
              ]
            }
            """);
        assertThat(hits.size(), is(1));
        Map<String, Object> fields = ((Map<String, Map<String, Object>>) hits.get(0)).get("fields");

        assertThat(fields.get("data_stream.type"), is(List.of("logs")));
        assertThat(fields.get("foo.attr"), is(List.of("bar")));
        assertThat(fields.get("attributes.foo.attr"), is(List.of("bar")));
        assertThat(fields.get("service.name"), is(List.of("my-service")));
        assertThat(fields.get("resource.attributes.service.name"), is(List.of("my-service")));
        assertThat(fields.get("@timestamp"), is(List.of("2023-07-03T14:34:24.123456789Z")));

        Map<String, Object> properties = getMappingProperties(client, getWriteBackingIndex(client, dataStream));
        assertThat(getValueFromPath(properties, List.of("@timestamp", "type")), is("date_nanos"));
        assertThat(
            getValueFromPath(properties, List.of("resource", "properties", "attributes", "properties", "service.name", "type")),
            is("keyword")
        );
        assertThat(
            getValueFromPath(
                properties,
                List.of("resource", "properties", "attributes", "properties", "service.name", "fields", "text", "type")
            ),
            is("match_only_text")
        );
        assertThat(getValueFromPath(properties, List.of("attributes", "properties", "complex.attribute", "type")), is("flattened"));
    }
}
