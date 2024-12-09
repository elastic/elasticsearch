/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class LogsIndexModeRestTestIT extends ESRestTestCase {
    protected static void waitForLogs(RestClient client) throws Exception {
        assertBusy(() -> {
            try {
                final Request request = new Request("GET", "_index_template/logs");
                assertOK(client.performRequest(request));
            } catch (ResponseException e) {
                fail(e.getMessage());
            }
        });
    }

    protected static Response putComponentTemplate(final RestClient client, final String componentTemplate, final String contends)
        throws IOException {
        final Request request = new Request("PUT", "/_component_template/" + componentTemplate);
        request.setJsonEntity(contends);
        if (isSyntheticSourceConfiguredInTemplate(contends) && SourceFieldMapper.onOrAfterDeprecateModeVersion(minimumIndexVersion())) {
            request.setOptions(
                expectVersionSpecificWarnings((VersionSensitiveWarningsHandler v) -> v.current(SourceFieldMapper.DEPRECATION_WARNING))
            );
        }
        return client.performRequest(request);
    }

    protected static Response putTemplate(final RestClient client, final String template, final String contents) throws IOException {
        final Request request = new Request("PUT", "/_index_template/" + template);
        request.setJsonEntity(contents);
        return client.performRequest(request);
    }

    protected static Response createDataStream(final RestClient client, final String dataStreamName) throws IOException {
        return client.performRequest(new Request("PUT", "_data_stream/" + dataStreamName));
    }

    protected static Response rolloverDataStream(final RestClient client, final String dataStreamName) throws IOException {
        return client.performRequest(new Request("POST", "/" + dataStreamName + "/_rollover"));
    }

    @SuppressWarnings("unchecked")
    protected static String getDataStreamBackingIndex(final RestClient client, final String dataStreamName, int backingIndex)
        throws IOException {
        final Request request = new Request("GET", "_data_stream/" + dataStreamName);
        final List<Object> dataStreams = (List<Object>) entityAsMap(client.performRequest(request)).get("data_streams");
        final Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
        final List<Map<String, String>> backingIndices = (List<Map<String, String>>) dataStream.get("indices");
        return backingIndices.get(backingIndex).get("index_name");
    }

    @SuppressWarnings("unchecked")
    protected static List<String> getDataStreamBackingIndices(final RestClient client, final String dataStreamName) throws IOException {
        final Request request = new Request("GET", "_data_stream/" + dataStreamName);
        final List<Object> dataStreams = (List<Object>) entityAsMap(client.performRequest(request)).get("data_streams");
        final Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
        final List<Map<String, String>> backingIndices = (List<Map<String, String>>) dataStream.get("indices");
        return backingIndices.stream().map(map -> map.get("indices")).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    protected static Object getSetting(final RestClient client, final String indexName, final String setting) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_settings?flat_settings=true&include_defaults=true");
        Map<String, Object> response = entityAsMap(client.performRequest(request));
        final Map<String, Object> settings = ((Map<String, Map<String, Object>>) response.get(indexName)).get("settings");
        final Map<String, Object> defaults = ((Map<String, Map<String, Object>>) response.get(indexName)).get("defaults");
        Object val = settings.get(setting);
        if (val == null) {
            val = defaults.get(setting);
        }
        return val;
    }

    protected static Response bulkIndex(final RestClient client, final String dataStreamName, final Supplier<String> bulkSupplier)
        throws IOException {
        var bulkRequest = new Request("POST", "/" + dataStreamName + "/_bulk");
        bulkRequest.setJsonEntity(bulkSupplier.get());
        bulkRequest.addParameter("refresh", "true");
        return client.performRequest(bulkRequest);
    }

    protected static Response putClusterSetting(final RestClient client, final String settingName, final Object settingValue)
        throws IOException {
        final Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{ \"transient\": { \"" + settingName + "\": " + settingValue + " } }");
        return client.performRequest(request);
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> getMapping(final RestClient client, final String indexName) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_mapping");

        Map<String, Object> mappings = ((Map<String, Map<String, Object>>) entityAsMap(client.performRequest(request)).get(indexName)).get(
            "mappings"
        );

        return mappings;
    }
}
