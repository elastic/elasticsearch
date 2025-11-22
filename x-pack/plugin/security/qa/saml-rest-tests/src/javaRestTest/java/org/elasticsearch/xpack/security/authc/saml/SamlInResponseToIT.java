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

    public void testNoInResponseTo() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        var response = authUser(username, null, null);
        assertThat(response, hasKey("access_token"));
    }

    public void testValidInResponseTo() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        var response = authUser(username, "some-request-id-54321", "some-request-id-54321");
        assertThat(response, hasKey("access_token"));
    }

    public void testInResponseToMismatch() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);

        var ex1 = expectThrows(ResponseException.class, () -> authUser(username, "r-54321", "r-12345"));
        assertThat(ex1.getResponse().getStatusLine().getStatusCode(), is(401));
        String errorEntity1 = EntityUtils.toString(ex1.getResponse().getEntity());
        assertThat(errorEntity1, containsString("\"security.saml.unsolicited_in_response_to\":\"r-12345\""));

        var ex2 = expectThrows(ResponseException.class, () -> authUser(username, null, "r-321"));
        assertThat(ex2.getResponse().getStatusLine().getStatusCode(), is(401));
        String errorEntity2 = EntityUtils.toString(ex2.getResponse().getEntity());
        assertThat(errorEntity2, containsString("\"security.saml.unsolicited_in_response_to\":\"r-321\""));

        var response = authUser(username, "54321", null);
        assertThat(response, hasKey("access_token"));
    }

    private Map<String, Object> authUser(String username, String inResponseToFromHeader, String inResponseToInSamlToken) throws Exception {
        final int realmNumber = 1;

        makeMetadataAvailable(realmNumber);
        var httpsAddress = getAcsHttpsAddress();
        var message = new SamlResponseBuilder().spEntityId("https://sp" + realmNumber + ".example.org/")
            .idpEntityId(getIdpEntityId(realmNumber))
            .acs(new URL("https://" + httpsAddress.getHostName() + ":" + httpsAddress.getPort() + "/acs/" + realmNumber))
            .attribute("urn:oid:2.5.4.3", username)
            .sign(getDataPath(SAML_SIGNING_CRT), getDataPath(SAML_SIGNING_KEY), new char[0])
            .inResponseTo(inResponseToInSamlToken)
            .asString();

        final Map<String, Object> body = new HashMap<>();
        body.put("content", Base64.getEncoder().encodeToString(message.getBytes(StandardCharsets.UTF_8)));
        body.put("realm", "saml" + realmNumber);
        if (inResponseToFromHeader != null) {
            body.put("ids", inResponseToFromHeader);
        }

        var req = new Request("POST", "_security/saml/authenticate");
        req.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().map(body)));
        var resp = entityAsMap(client().performRequest(req));
        assertThat(resp, hasEntry("username", username));
        assertThat(ObjectPath.evaluate(resp, "authentication.authentication_realm.name"), equalTo("saml" + realmNumber));
        return resp;
    }
}
