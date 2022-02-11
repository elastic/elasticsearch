/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test;

import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;

import java.io.IOException;

public class SecurityClientTestHelper {

    public static void setUserEnabled(RestClient client, String username, boolean enabled) throws IOException {
        final String endpoint = "/_security/user/" + username + "/" + (enabled ? "_enable" : "_disable");
        final Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        request.setOptions(SecuritySettingsSource.SECURITY_REQUEST_OPTIONS);
        client.performRequest(request);
    }
}
