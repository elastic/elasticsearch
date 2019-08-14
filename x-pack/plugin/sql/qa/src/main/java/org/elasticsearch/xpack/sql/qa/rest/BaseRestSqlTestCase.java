/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.sql.proto.StringUtils;

import java.io.IOException;

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
}
