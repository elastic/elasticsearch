/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.rest;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.cbor.CborXContent;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.sql.proto.Protocol.BINARY_FORMAT_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.CLIENT_ID_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.VERSION_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.COLUMNAR_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.CURSOR_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.FETCH_SIZE_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.FILTER_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.MODE_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.PARAMS_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.QUERY_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.TIME_ZONE_NAME;

public abstract class BaseRestSqlTestCase extends ESRestTestCase {

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
                    version(Version.CURRENT.toString());
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
                if (lowerName.equals(PARAMS_NAME) || lowerName.equals(FILTER_NAME)) {
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
        Request request = new Request("POST", "/test/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (String doc : docs) {
            bulk.append("{\"index\":{}\n");
            bulk.append(doc + "\n");
        }
        request.setJsonEntity(bulk.toString());
        client().performRequest(request);
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
}
