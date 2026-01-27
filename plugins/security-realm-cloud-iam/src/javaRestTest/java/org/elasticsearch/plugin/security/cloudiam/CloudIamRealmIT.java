/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License v 1".
 */
package org.elasticsearch.plugin.security.cloudiam;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CloudIamRealmIT extends ESRestTestCase {
    public void testAuthenticateWithMockHeader() throws IOException {
        Request request = new Request("GET", "/_security/_authenticate");
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.addHeader("X-ES-IAM-Signed", signedHeader("mock", "nonce-1"));
        request.setOptions(options);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        String body = EntityUtils.toString(response.getEntity());
        assertThat(body, containsString("cloud_arn"));
    }

    public void testRejectsInvalidSignature() throws IOException {
        Request request = new Request("GET", "/_security/_authenticate");
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.addHeader("X-ES-IAM-Signed", signedHeader("bad", "nonce-2"));
        request.setOptions(options);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(401));
    }

    private String signedHeader(String signature, String nonce) {
        String timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS).toString();
        String json = "{"
            + "\"Action\":\"GetCallerIdentity\","
            + "\"Version\":\"2015-04-01\","
            + "\"AccessKeyId\":\"AKID\","
            + "\"Signature\":\"" + signature + "\","
            + "\"SignatureMethod\":\"HMAC-SHA1\","
            + "\"SignatureVersion\":\"1.0\","
            + "\"SignatureNonce\":\"" + nonce + "\","
            + "\"Timestamp\":\"" + timestamp + "\""
            + "}";
        return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }
}
