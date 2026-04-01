/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SamlServiceProviderMetadataIT extends SamlRestTestCase {

    public void testAuthenticationWhenMetadataIsUnreliable() throws Exception {
        // Start with no metadata available
        makeAllMetadataUnavailable();

        final String username = randomAlphaOfLengthBetween(4, 12);
        for (int realmNumber : shuffledList(List.of(1, 2, 3))) {
            // Authc fails because metadata has never been loaded.
            var ex = expectThrows(ResponseException.class, () -> samlAuthUser(realmNumber, username));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(401));

            // Authc works once metadata is available.
            makeMetadataAvailable(realmNumber);
            samlAuthUser(realmNumber, username);
        }

        // Switch off all metadata
        makeAllMetadataUnavailable();
        for (int realmNumber : List.of(1, 2, 3)) {
            // Authc still works because metadata is cached.
            samlAuthUser(realmNumber, username);
        }
    }

    private void samlAuthUser(int realmNumber, String username) throws Exception {
        var httpsAddress = getAcsHttpsAddress();
        var message = new SamlResponseBuilder().spEntityId("https://sp" + realmNumber + ".example.org/")
            .idpEntityId(getIdpEntityId(realmNumber))
            .acs(new URL("https://" + httpsAddress.getHostName() + ":" + httpsAddress.getPort() + "/acs/" + realmNumber))
            .attribute("urn:oid:2.5.4.3", username)
            .sign(getDataPath(SAML_SIGNING_CRT), getDataPath(SAML_SIGNING_KEY), new char[0])
            .asString();

        final Map<String, Object> body = new HashMap<>();
        body.put("content", Base64.getEncoder().encodeToString(message.getBytes(StandardCharsets.UTF_8)));
        if (randomBoolean()) {
            // If realm is not specified the action will infer it based on the ACS in the saml auth message
            body.put("realm", getSamlRealmName(realmNumber));
        }
        var req = new Request("POST", "_security/saml/authenticate");
        req.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().map(body)));
        var resp = entityAsMap(client().performRequest(req));
        assertThat(resp.get("username"), equalTo(username));
        assertThat(ObjectPath.evaluate(resp, "authentication.authentication_realm.name"), equalTo(getSamlRealmName(realmNumber)));
    }
}
