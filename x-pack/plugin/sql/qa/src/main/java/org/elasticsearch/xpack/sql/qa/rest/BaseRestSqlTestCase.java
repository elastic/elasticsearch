/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.cbor.CborXContent;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public abstract class BaseRestSqlTestCase extends ESRestTestCase {
    
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

    public static String mode(String mode) {
        return Strings.isEmpty(mode) ? StringUtils.EMPTY : ",\"mode\":\"" + mode + "\"";
    }

    public static String randomMode() {
        return randomFrom(StringUtils.EMPTY, "jdbc", "plain");
    }

    /**
     * JSON parser returns floating point numbers as Doubles, while CBOR as their actual type.
     * To have the tests compare the correct data type, the floating point numbers types should be passed accordingly, to the comparators.
     */
    public static Number xContentDependentFloatingNumberValue(String mode, Number value) {
        Mode m = Mode.fromString(mode);
        // for drivers and the CLI return the number as is, while for REST cast it implicitly to Double (the JSON standard).
        if (Mode.isDriver(m) || m == Mode.CLI) {
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
