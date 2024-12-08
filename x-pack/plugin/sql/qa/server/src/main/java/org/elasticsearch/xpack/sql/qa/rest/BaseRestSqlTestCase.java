/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.rest;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.cbor.CborXContent;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.elasticsearch.xpack.sql.proto.SqlVersions;
import org.elasticsearch.xpack.sql.proto.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.sql.proto.CoreProtocol.ALLOW_PARTIAL_SEARCH_RESULTS_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.BINARY_FORMAT_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.CATALOG_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.CLIENT_ID_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.COLUMNAR_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.CURSOR_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.FETCH_SIZE_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.FIELD_MULTI_VALUE_LENIENCY_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.FILTER_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.INDEX_INCLUDE_FROZEN_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.KEEP_ALIVE_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.KEEP_ON_COMPLETION_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.MODE_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.PARAMS_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.QUERY_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.RUNTIME_MAPPINGS_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.TIME_ZONE_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.VERSION_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.WAIT_FOR_COMPLETION_TIMEOUT_NAME;
import static org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase.SQL_QUERY_REST_ENDPOINT;

public abstract class BaseRestSqlTestCase extends RemoteClusterAwareSqlRestTestCase {

    private static final String TEST_INDEX = "test";
    private static final String DATA_STREAM_TEMPLATE = "test-ds-index-template";
    /**
     * What's the version of the server that the clients should be compatible with?
     * This will be either the stack version, or SqlVersions.getLatestVersion() if the stack version is not available.
     */
    private static final SqlVersion SERVER_COMPAT_VERSION = getServerCompatVersion();

    public static class RequestObjectBuilder {
        private StringBuilder request;
        private final boolean isQuery;

        private RequestObjectBuilder(String init, boolean isQuery) {
            request = new StringBuilder(init);
            this.isQuery = isQuery;
        }

        public static RequestObjectBuilder query(String query) {
            return new RequestObjectBuilder(field(QUERY_NAME, query).substring(1), true);
        }

        public static RequestObjectBuilder cursor(String cursor) {
            return new RequestObjectBuilder(field(CURSOR_NAME, cursor).substring(1), false);
        }

        public RequestObjectBuilder version(String version) {
            request.append(field(VERSION_NAME, version));
            return this;
        }

        public RequestObjectBuilder mode(Object m) {
            String modeString = m.toString();
            request.append(field(MODE_NAME, modeString));
            if (isQuery) {
                Mode mode = (m instanceof Mode) ? (Mode) m : Mode.fromString(modeString);
                if (Mode.isDedicatedClient(mode)) {
                    version(SERVER_COMPAT_VERSION.toString());
                }
            }
            return this;
        }

        public RequestObjectBuilder fetchSize(Integer fetchSize) {
            request.append(field(FETCH_SIZE_NAME, fetchSize));
            return this;
        }

        public RequestObjectBuilder timeZone(String timeZone) {
            request.append(field(TIME_ZONE_NAME, timeZone));
            return this;
        }

        public RequestObjectBuilder catalog(String catalog) {
            request.append(field(CATALOG_NAME, catalog));
            return this;
        }

        public RequestObjectBuilder clientId(String clientId) {
            request.append(field(CLIENT_ID_NAME, clientId));
            return this;
        }

        public RequestObjectBuilder filter(String filter) {
            request.append(field(FILTER_NAME, filter));
            return this;
        }

        public RequestObjectBuilder params(String params) {
            request.append(field(PARAMS_NAME, params));
            return this;
        }

        public RequestObjectBuilder columnar(Boolean columnar) {
            request.append(field(COLUMNAR_NAME, columnar));
            return this;
        }

        public RequestObjectBuilder binaryFormat(Boolean binaryFormat) {
            request.append(field(BINARY_FORMAT_NAME, binaryFormat));
            return this;
        }

        public RequestObjectBuilder waitForCompletionTimeout(String timeout) {
            request.append(field(WAIT_FOR_COMPLETION_TIMEOUT_NAME, timeout));
            return this;
        }

        public RequestObjectBuilder keepOnCompletion(Boolean keepOnCompletion) {
            request.append(field(KEEP_ON_COMPLETION_NAME, keepOnCompletion));
            return this;
        }

        public RequestObjectBuilder keepAlive(String keepAlive) {
            request.append(field(KEEP_ALIVE_NAME, keepAlive));
            return this;
        }

        public RequestObjectBuilder fieldMultiValueLeniency(Boolean fieldMultiValueLeniency) {
            request.append(field(FIELD_MULTI_VALUE_LENIENCY_NAME, fieldMultiValueLeniency));
            return this;
        }

        public RequestObjectBuilder runtimeMappings(String runtimeMappings) {
            request.append(field(RUNTIME_MAPPINGS_NAME, runtimeMappings));
            return this;
        }

        public RequestObjectBuilder indexIncludeFrozen(boolean includeFrozen) {
            request.append(field(INDEX_INCLUDE_FROZEN_NAME, includeFrozen));
            return this;
        }

        public RequestObjectBuilder allowPartialSearchResults(boolean allowPartialSearchResults) {
            request.append(field(ALLOW_PARTIAL_SEARCH_RESULTS_NAME, allowPartialSearchResults));
            return this;
        }

        private static String field(String name, Object value) {
            if (value == null) {
                return StringUtils.EMPTY;
            }

            String field = "\"" + name + "\":";
            if (value instanceof String) {
                if (((String) value).isEmpty()) {
                    return StringUtils.EMPTY;
                }
                String lowerName = name.toLowerCase(Locale.ROOT);
                if (lowerName.equals(PARAMS_NAME) || lowerName.equals(FILTER_NAME) || lowerName.equals(RUNTIME_MAPPINGS_NAME)) {
                    field += value;
                } else {
                    field += "\"" + value + "\"";
                }
            } else {
                field += value;
            }
            return "," + field;
        }

        @Override
        public String toString() {
            return "{" + request.toString() + "}";
        }
    }

    protected void index(String... docs) throws IOException {
        indexWithIndexName(TEST_INDEX, docs);
    }

    protected void indexWithIndexName(String indexName, String... docs) throws IOException {
        Request request = new Request("POST", "/" + indexName + "/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (String doc : docs) {
            bulk.append(String.format(Locale.ROOT, """
                {"index":{}}
                %s
                """, doc));
        }
        request.setJsonEntity(bulk.toString());
        provisioningClient().performRequest(request);
    }

    // can be used with regular indices as well as data streams (that would need a "create":{} instead of an "index":{} bulk operation)
    protected void indexWithIndexName(String indexName, String doc) throws IOException {
        Request request = new Request("POST", "/" + indexName + "/_doc");
        request.addParameter("refresh", "true");
        request.setJsonEntity(doc);
        provisioningClient().performRequest(request);
    }

    protected void deleteTestIndex() throws IOException {
        deleteIndexWithProvisioningClient(TEST_INDEX);
    }

    protected void deleteIndexWithProvisioningClient(String name) throws IOException {
        deleteIndex(provisioningClient(), name);
    }

    public static void createDataStream(String dataStreamName, RestClient provisioningClient) throws IOException {
        Request request = new Request("PUT", "/_index_template/" + DATA_STREAM_TEMPLATE + "-" + dataStreamName);
        request.setJsonEntity("{\"index_patterns\": [\"" + dataStreamName + "*\"], \"data_stream\": {}}");
        assertOK(provisioningClient.performRequest(request));

        request = new Request("PUT", "/_data_stream/" + dataStreamName);
        assertOK(provisioningClient.performRequest(request));
    }

    public void createDataStream(String dataStreamName) throws IOException {
        createDataStream(dataStreamName, provisioningClient());
    }

    public static void deleteDataStream(String dataStreamName, RestClient provisioningClient) throws IOException {
        Request request = new Request("DELETE", "_data_stream/" + dataStreamName);
        provisioningClient.performRequest(request);
        request = new Request("DELETE", "/_index_template/" + DATA_STREAM_TEMPLATE + "-" + dataStreamName);
        provisioningClient.performRequest(request);
    }

    public void deleteDataStream(String dataStreamName) throws IOException {
        deleteDataStream(dataStreamName, provisioningClient());
    }

    public static RequestObjectBuilder query(String query) {
        return RequestObjectBuilder.query(query);
    }

    public static RequestObjectBuilder cursor(String query) {
        return RequestObjectBuilder.cursor(query);
    }

    public static String randomMode() {
        return randomFrom(StringUtils.EMPTY, Mode.JDBC.toString(), Mode.PLAIN.toString());
    }

    /**
     * JSON parser returns floating point numbers as Doubles, while CBOR as their actual type.
     * To have the tests compare the correct data type, the floating point numbers types should be passed accordingly, to the comparators.
     */
    public static Number xContentDependentFloatingNumberValue(String mode, Number value) {
        Mode m = Mode.fromString(mode);
        // for drivers and the CLI return the number as is, while for REST cast it implicitly to Double (the JSON standard).
        if (Mode.isDedicatedClient(m)) {
            return value;
        } else {
            return value.doubleValue();
        }
    }

    public static Map<String, Object> toMap(Response response, String mode) throws IOException {
        Mode m = Mode.fromString(mode);
        try (InputStream content = response.getEntity().getContent()) {
            // by default, drivers and the CLI respond in binary format
            if (Mode.isDriver(m) || m == Mode.CLI) {
                return XContentHelper.convertToMap(CborXContent.cborXContent, content, false);
            } else {
                return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
            }
        }
    }

    public static Tuple<String, String> runSqlAsText(RequestObjectBuilder requestObject, String format) throws IOException {
        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        request.addParameter("error_trace", "true");
        request.setEntity(new StringEntity(requestObject.toString(), ContentType.APPLICATION_JSON));
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Accept", format);
        request.setOptions(options);
        Response response = client().performRequest(request);
        return new Tuple<>(
            Streams.copyToString(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8)),
            response.getHeader("Cursor")
        );
    }

    private static SqlVersion getServerCompatVersion() {
        try {
            return SqlVersion.fromString(Build.current().version());
        } catch (Exception e) {
            return SqlVersions.getLatestVersion();
        }
    }
}
