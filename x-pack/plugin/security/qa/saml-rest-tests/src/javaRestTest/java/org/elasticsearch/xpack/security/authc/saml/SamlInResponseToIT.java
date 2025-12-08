/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.BeforeClass;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public class SamlInResponseToIT extends SamlRestTestCase {

    private static final int REALM_NUMBER = 1;

    public void testInResponseTo_matchingValues() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        String requestId = generateRandomRequestId();
        var response = authUser(username, requestId, requestId);
        assertThat(response, hasKey("access_token"));
    }

    public void testInResponseTo_requestAndTokenHaveDifferentValues() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        String requestIdFromRequest = generateRandomRequestId();
        String requestIdFromToken = generateDifferentRandomRequestId(requestIdFromRequest);

        var exception = expectThrows(ResponseException.class, () -> authUser(username, requestIdFromRequest, requestIdFromToken));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(401));
        String errorEntity = EntityUtils.toString(exception.getResponse().getEntity());
        assertThat(errorEntity, containsString("\"security.saml.unsolicited_in_response_to\":\"" + requestIdFromToken + "\""));
    }

    public void testInResponseTo_requestNullTokenNotNull() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        String requestIdFromToken = generateRandomRequestId();

        var exception = expectThrows(ResponseException.class, () -> authUser(username, null, requestIdFromToken));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(401));
        String errorEntity = EntityUtils.toString(exception.getResponse().getEntity());
        assertThat(errorEntity, containsString("\"security.saml.unsolicited_in_response_to\":\"" + requestIdFromToken + "\""));
    }

    public void testInResponseTo_requestNotNullTokenNull() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        String requestIdFromRequest = generateRandomRequestId();

        var response = authUser(username, requestIdFromRequest, null);
        assertThat(response, hasKey("access_token"));
    }

    public void testInResponseTo_requestNullTokenNull() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        var response = authUser(username, null, null);
        assertThat(response, hasKey("access_token"));
    }

    private String generateRandomRequestId() {
        return randomAlphaOfLength(1) + randomAlphanumericOfLength(random().nextInt(10));
    }

    private String generateDifferentRandomRequestId(String existingId) {
        String newId;
        do {
            newId = generateRandomRequestId();
        } while (newId.equals(existingId));
        return newId;
    }

    private Map<String, Object> authUser(String username, String inResponseToFromHeader, String inResponseToInSamlToken) throws Exception {

        var httpsAddress = getAcsHttpsAddress();
        var message = new SamlResponseBuilder().spEntityId("https://sp" + REALM_NUMBER + ".example.org/")
            .idpEntityId(getIdpEntityId(REALM_NUMBER))
            .acs(new URL("https://" + httpsAddress.getHostName() + ":" + httpsAddress.getPort() + "/acs/" + REALM_NUMBER))
            .attribute("urn:oid:2.5.4.3", username)
            .sign(getDataPath(SAML_SIGNING_CRT), getDataPath(SAML_SIGNING_KEY), new char[0])
            .inResponseTo(inResponseToInSamlToken)
            .asString();

        final Map<String, Object> body = new HashMap<>();
        body.put("content", Base64.getEncoder().encodeToString(message.getBytes(StandardCharsets.UTF_8)));
        body.put("realm", getSamlRealmName(REALM_NUMBER));
        if (inResponseToFromHeader != null) {
            body.put("ids", inResponseToFromHeader);
        }

        var req = new Request("POST", "_security/saml/authenticate");
        req.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().map(body)));
        var resp = entityAsMap(client().performRequest(req));
        assertThat(resp, hasEntry("username", username));
        assertThat(ObjectPath.evaluate(resp, "authentication.authentication_realm.name"), equalTo(getSamlRealmName(REALM_NUMBER)));
        return resp;
    }
}
