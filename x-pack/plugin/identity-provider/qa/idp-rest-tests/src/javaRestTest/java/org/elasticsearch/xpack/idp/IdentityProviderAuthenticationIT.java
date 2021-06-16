/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.action.saml.SamlPrepareAuthenticationResponse;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class IdentityProviderAuthenticationIT extends IdpRestTestCase {

    // From build.gradle
    private final String SP_ENTITY_ID = "ec:123456:abcdefg";
    private final String SP_ACS = "https://sp1.test.es.elasticsearch.org/saml/acs";
    private final String REALM_NAME = "cloud-saml";

    @Before
    public void setupSecurityData() throws IOException {
        setUserPassword("kibana_system", new SecureString("kibana_system".toCharArray()));
        createApplicationPrivileges("elastic-cloud", Map.ofEntries(
            Map.entry("deployment_admin", Set.of("sso:admin")),
            Map.entry("deployment_viewer", Set.of("sso:viewer"))
        ));
    }

    public void testRegistrationAndIdpInitiatedSso() throws Exception {
        final Map<String, Object> request = Map.ofEntries(
            Map.entry("name", "Test SP"),
            Map.entry("acs", SP_ACS),
            Map.entry("privileges", Map.ofEntries(
                Map.entry("resource", SP_ENTITY_ID),
                Map.entry("roles", List.of("sso:(\\w+)"))
            )),
            Map.entry("attributes", Map.ofEntries(
                Map.entry("principal", "https://idp.test.es.elasticsearch.org/attribute/principal"),
                Map.entry("name", "https://idp.test.es.elasticsearch.org/attribute/name"),
                Map.entry("email", "https://idp.test.es.elasticsearch.org/attribute/email"),
                Map.entry("roles", "https://idp.test.es.elasticsearch.org/attribute/roles")
            )));
        final SamlServiceProviderIndex.DocumentVersion docVersion = createServiceProvider(SP_ENTITY_ID, request);
        checkIndexDoc(docVersion);
        ensureGreen(SamlServiceProviderIndex.INDEX_NAME);
        final String samlResponse = generateSamlResponse(SP_ENTITY_ID, SP_ACS, null);
        authenticateWithSamlResponse(samlResponse, null);
    }

    public void testRegistrationAndSpInitiatedSso() throws Exception {
        final Map<String, Object> request = Map.ofEntries(
            Map.entry("name", "Test SP"),
            Map.entry("acs", SP_ACS),
            Map.entry("privileges", Map.ofEntries(
                Map.entry("resource", SP_ENTITY_ID),
                Map.entry("roles", List.of("sso:(\\w+)"))
            )),
            Map.entry("attributes", Map.ofEntries(
                Map.entry("principal", "https://idp.test.es.elasticsearch.org/attribute/principal"),
                Map.entry("name", "https://idp.test.es.elasticsearch.org/attribute/name"),
                Map.entry("email", "https://idp.test.es.elasticsearch.org/attribute/email"),
                Map.entry("roles", "https://idp.test.es.elasticsearch.org/attribute/roles")
            )));
        final SamlServiceProviderIndex.DocumentVersion docVersion = createServiceProvider(SP_ENTITY_ID, request);
        checkIndexDoc(docVersion);
        ensureGreen(SamlServiceProviderIndex.INDEX_NAME);
        SamlPrepareAuthenticationResponse samlPrepareAuthenticationResponse = generateSamlAuthnRequest(REALM_NAME);
        final String requestId = samlPrepareAuthenticationResponse.getRequestId();
        final String query = samlPrepareAuthenticationResponse.getRedirectUrl().split("\\?")[1];
        Map<String, Object> authnState = validateAuthnRequest(SP_ENTITY_ID, query);
        final String samlResponse = generateSamlResponse(SP_ENTITY_ID, SP_ACS, authnState);
        assertThat(samlResponse, containsString("InResponseTo=\"" + requestId + "\""));
        authenticateWithSamlResponse(samlResponse, requestId);
    }

    private Map<String, Object> validateAuthnRequest(String entityId, String authnRequestQuery) throws Exception {
        final Request request = new Request("POST", "/_idp/saml/validate");
        request.setJsonEntity("{\"authn_request_query\":\"" + authnRequestQuery + "\"}");
        final Response response = client().performRequest(request);
        final Map<String, Object> map = entityAsMap(response);
        assertThat(ObjectPath.eval("service_provider.entity_id", map), instanceOf(String.class));
        assertThat(ObjectPath.eval("service_provider.entity_id", map), equalTo(entityId));
        assertThat(ObjectPath.eval("authn_state", map), instanceOf(Map.class));
        return ObjectPath.eval("authn_state", map);
    }

    private SamlPrepareAuthenticationResponse generateSamlAuthnRequest(String realmName) throws Exception {
        final Request request = new Request("POST", "/_security/saml/prepare");
        request.setJsonEntity("{\"realm\":\"" + realmName + "\"}");
        try (RestClient kibanaClient = restClientAsKibanaSystem()) {
            final Response response = kibanaClient.performRequest(request);
            final Map<String, Object> map = entityAsMap(response);
            assertThat(ObjectPath.eval("realm", map), equalTo(realmName));
            assertThat(ObjectPath.eval("id", map), instanceOf(String.class));
            assertThat(ObjectPath.eval("redirect", map), instanceOf(String.class));
            return new SamlPrepareAuthenticationResponse(realmName, ObjectPath.eval("id", map), ObjectPath.eval("redirect", map));
        }
    }

    private String generateSamlResponse(String entityId, String acs, @Nullable Map<String, Object> authnState) throws Exception {
        final Request request = new Request("POST", "/_idp/saml/init");
        if (authnState != null && authnState.isEmpty() == false) {
            request.setJsonEntity("{\"entity_id\":\"" + entityId + "\", \"acs\":\"" + acs + "\"," +
                "\"authn_state\":" + Strings.toString(JsonXContent.contentBuilder().map(authnState)) + "}");
        } else {
            request.setJsonEntity("{\"entity_id\":\"" + entityId + "\", \"acs\":\"" + acs + "\"}");
        }
        request.setOptions(RequestOptions.DEFAULT.toBuilder()
            .addHeader("es-secondary-authorization", basicAuthHeaderValue("idp_user",
                new SecureString("idp-password".toCharArray())))
            .build());
        final Response response = client().performRequest(request);
        final Map<String, Object> map = entityAsMap(response);
        assertThat(ObjectPath.eval("service_provider.entity_id", map), equalTo(entityId));
        assertThat(ObjectPath.eval("post_url", map), equalTo(acs));
        assertThat(ObjectPath.eval("saml_response", map), instanceOf(String.class));
        return (String) ObjectPath.eval("saml_response", map);
    }

    private void authenticateWithSamlResponse(String samlResponse, @Nullable String id) throws Exception {
        final String encodedResponse = Base64.getEncoder().encodeToString(samlResponse.getBytes(StandardCharsets.UTF_8));
        final Request request = new Request("POST", "/_security/saml/authenticate");
        if (Strings.hasText(id)) {
            request.setJsonEntity("{\"content\":\"" + encodedResponse + "\", \"realm\":\"" + REALM_NAME + "\", \"ids\":[\"" + id + "\"]}");
        } else {
            request.setJsonEntity("{\"content\":\"" + encodedResponse + "\", \"realm\":\"" + REALM_NAME + "\"}");
        }
        final String accessToken;
        try (RestClient kibanaClient = restClientAsKibanaSystem()) {
            final Response response = kibanaClient.performRequest(request);
            final Map<String, Object> map = entityAsMap(response);
            assertThat(ObjectPath.eval("username", map), instanceOf(String.class));
            assertThat(ObjectPath.eval("username", map), equalTo("idp_user"));
            assertThat(ObjectPath.eval("realm", map), instanceOf(String.class));
            assertThat(ObjectPath.eval("realm", map), equalTo(REALM_NAME));
            assertThat(ObjectPath.eval("access_token", map), instanceOf(String.class));
            accessToken = ObjectPath.eval("access_token", map);
            assertThat(ObjectPath.eval("refresh_token", map), instanceOf(String.class));
        }
        try (RestClient accessTokenClient = restClientWithToken(accessToken)) {
            final Request authenticateRequest = new Request("GET", "/_security/_authenticate");
            final Response authenticateResponse = accessTokenClient.performRequest(authenticateRequest);
            final Map<String, Object> authMap = entityAsMap(authenticateResponse);
            assertThat(ObjectPath.eval("username", authMap), instanceOf(String.class));
            assertThat(ObjectPath.eval("username", authMap), equalTo("idp_user"));
            assertThat(ObjectPath.eval("metadata.saml_nameid_format", authMap), instanceOf(String.class));
            assertThat(ObjectPath.eval("metadata.saml_nameid_format", authMap),
                equalTo("urn:oasis:names:tc:SAML:2.0:nameid-format:transient"));
            assertThat(ObjectPath.eval("metadata.saml_roles", authMap), instanceOf(List.class));
            assertThat(ObjectPath.eval("metadata.saml_roles", authMap), hasSize(1));
            assertThat(ObjectPath.eval("metadata.saml_roles", authMap), contains("viewer"));
        }
    }

    private RestClient restClientWithToken(String accessToken) throws IOException {
        return buildClient(
            Settings.builder().put(ThreadContext.PREFIX + ".Authorization", "Bearer " + accessToken).build(),
            getClusterHosts().toArray(new HttpHost[getClusterHosts().size()]));
    }

    private RestClient restClientAsKibanaSystem() throws IOException {
        return buildClient(
            Settings.builder().put(ThreadContext.PREFIX + ".Authorization", basicAuthHeaderValue("kibana_system",
                new SecureString("kibana_system".toCharArray()))).build(),
            getClusterHosts().toArray(new HttpHost[getClusterHosts().size()]));
    }
}

