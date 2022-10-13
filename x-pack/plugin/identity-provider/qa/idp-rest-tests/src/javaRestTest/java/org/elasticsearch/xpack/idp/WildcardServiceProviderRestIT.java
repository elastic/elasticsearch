/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class WildcardServiceProviderRestIT extends IdpRestTestCase {

    // From build.gradle
    private final String IDP_ENTITY_ID = "https://idp.test.es.elasticsearch.org/";
    // From SAMLConstants
    private final String REDIRECT_BINDING = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect";

    @Before
    public void defineApplicationPrivileges() throws IOException {
        super.createApplicationPrivileges(
            "elastic-cloud",
            Map.ofEntries(Map.entry("deployment_admin", Set.of("sso:admin")), Map.entry("deployment_viewer", Set.of("sso:viewer")))
        );
    }

    public void testGetWildcardServiceProviderMetadata() throws Exception {
        final String owner = randomAlphaOfLength(8);
        final String service = randomAlphaOfLength(8);
        // From "wildcard_services.json"
        final String entityId = "service:" + owner + ":" + service;
        final String acs = "https://" + service + ".services.example.com/saml/acs";
        getMetadata(entityId, acs);
    }

    public void testInitSingleSignOnToWildcardServiceProvider() throws Exception {
        final String owner = randomAlphaOfLength(8);
        final String service = randomAlphaOfLength(8);
        // From "wildcard_services.json"
        final String entityId = "service:" + owner + ":" + service;
        final String acs = "https://" + service + ".services.example.com/api/v1/saml";

        final String username = randomAlphaOfLength(6);
        final SecureString password = new SecureString((randomAlphaOfLength(6) + randomIntBetween(10, 99)).toCharArray());
        final String roleName = username + "_role";
        final User user = createUser(username, password, roleName);

        final RoleDescriptor.ApplicationResourcePrivileges applicationPrivilege = RoleDescriptor.ApplicationResourcePrivileges.builder()
            .application("elastic-cloud")
            .privileges("sso:admin")
            .resources("sso:" + entityId)
            .build();
        createRole(roleName, List.of(), List.of(), List.of(applicationPrivilege));

        final String samlResponse = initSso(entityId, acs, new UsernamePasswordToken(username, password));

        for (String attr : List.of("principal", "email", "name", "roles")) {
            assertThat(samlResponse, containsString("Name=\"saml:attribute:" + attr + "\""));
            assertThat(samlResponse, containsString("FriendlyName=\"" + attr + "\""));
        }

        assertThat(samlResponse, containsString(user.principal()));
        assertThat(samlResponse, containsString(user.email()));
        assertThat(samlResponse, containsString(user.fullName()));
        assertThat(samlResponse, containsString(">admin<"));

        deleteUser(username);
        deleteRole(roleName);
    }

    private void getMetadata(String entityId, String acs) throws IOException {
        final Map<String, Object> map = getAsMap("/_idp/saml/metadata/" + encode(entityId) + "?acs=" + encode(acs));
        assertThat(map, notNullValue());
        assertThat(map.keySet(), containsInAnyOrder("metadata"));
        final Object metadata = map.get("metadata");
        assertThat(metadata, notNullValue());
        assertThat(metadata, instanceOf(String.class));
        assertThat((String) metadata, containsString(IDP_ENTITY_ID));
        assertThat((String) metadata, containsString(REDIRECT_BINDING));
    }

    private String initSso(String entityId, String acs, UsernamePasswordToken secondaryAuth) throws IOException {
        final Request request = new Request("POST", "/_idp/saml/init/");
        request.setJsonEntity(toJson(Map.of("entity_id", entityId, "acs", acs)));
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .addHeader(
                    "es-secondary-authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(secondaryAuth.principal(), secondaryAuth.credentials())
                )
        );
        Response response = client().performRequest(request);

        final Map<String, Object> map = entityAsMap(response);
        assertThat(map, notNullValue());
        assertThat(map.keySet(), containsInAnyOrder("post_url", "saml_response", "saml_status", "service_provider", "error"));
        assertThat(map.get("post_url"), equalTo(acs));
        assertThat(map.get("saml_response"), instanceOf(String.class));

        final String samlResponse = (String) map.get("saml_response");
        assertThat(samlResponse, containsString(entityId));
        assertThat(samlResponse, containsString(acs));

        return samlResponse;
    }

    private String toJson(Map<String, Object> body) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent()).map(body)) {
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

}
