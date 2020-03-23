/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp;

import org.apache.http.HttpHost;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class IdentityProviderAuthenticationIT extends IdpRestTestCase {

    // From build.gradle
    private final String SP_ENTITY_ID = "ec:123456:abcdefg";
    private final String SP_ACS = "https://sp1.test.es.elasticsearch.org/saml/acs";

    public void testRegisterAndIdpInitiatedSso() throws Exception {
        final Map<String, Object> request = Map.ofEntries(
            Map.entry("name", "Test SP"),
            Map.entry("acs", SP_ACS),
            Map.entry("privileges", Map.ofEntries(
                Map.entry("resource", SP_ENTITY_ID),
                Map.entry("roles", Map.of("superuser", "role:superuser", "viewer", "role:viewer"))
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
        final String samlResponse = generateSamlResponse(SP_ENTITY_ID, SP_ACS);
        authenticateWithSamlResponse(samlResponse);
    }

    private void checkIndexDoc(SamlServiceProviderIndex.DocumentVersion docVersion) throws IOException {
        final Request request = new Request("GET", SamlServiceProviderIndex.INDEX_NAME + "/_doc/" + docVersion.id);
        final Response response = adminClient().performRequest(request);
        final Map<String, Object>
            map = entityAsMap(response);
        assertThat(map.get("_index"), equalTo(SamlServiceProviderIndex.INDEX_NAME));
        assertThat(map.get("_id"), equalTo(docVersion.id));
        assertThat(asLong(map.get("_seq_no")), equalTo(docVersion.seqNo));
        assertThat(asLong(map.get("_primary_term")), equalTo(docVersion.primaryTerm));
    }

    private SamlServiceProviderIndex.DocumentVersion createServiceProvider(String entityId, Map<String, Object> body) throws IOException {
        final Request request =
            new Request("PUT", "/_idp/saml/sp/" + encode(entityId) + "?refresh=" + WriteRequest.RefreshPolicy.IMMEDIATE.getValue());
        final String entity = Strings.toString(JsonXContent.contentBuilder().map(body));
        request.setJsonEntity(entity);
        final Response response = client().performRequest(request);
        final Map<String, Object> map = entityAsMap(response);
        assertThat(ObjectPath.eval("service_provider.entity_id", map), equalTo(entityId));
        assertThat(ObjectPath.eval("service_provider.enabled", map), equalTo(true));

        final Object docId = ObjectPath.eval("document._id", map);
        final Object seqNo = ObjectPath.eval("document._seq_no", map);
        final Object primaryTerm = ObjectPath.eval("document._primary_term", map);
        assertThat(docId, instanceOf(String.class));
        assertThat(seqNo, instanceOf(Number.class));
        assertThat(primaryTerm, instanceOf(Number.class));
        return new SamlServiceProviderIndex.DocumentVersion((String) docId, asLong(primaryTerm), asLong(seqNo));
    }

    private String generateSamlResponse(String entityId, String acs) throws Exception {
        final Request request = new Request("POST", "/_idp/saml/init");
        request.setJsonEntity("{\"entity_id\":\"" + entityId + "\"}");
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


    private void authenticateWithSamlResponse(String samlResponse) throws Exception{
        final String encodedResponse = Base64.getEncoder().encodeToString(samlResponse.getBytes(StandardCharsets.UTF_8));
        final Request request = new Request("POST", "/_security/saml/authenticate");
        request.setJsonEntity("{\"content\":\""+encodedResponse+"\", \"realm\":\"cloud-saml\"}");
        final Response response = client().performRequest(request);
        final Map<String, Object> map = entityAsMap(response);
        assertThat(ObjectPath.eval("username", map), instanceOf(String.class));
        assertThat(ObjectPath.eval("username", map), equalTo("idp_user"));
        assertThat(ObjectPath.eval("realm", map), instanceOf(String.class));
        assertThat(ObjectPath.eval("realm", map), equalTo("cloud-saml"));
        assertThat(ObjectPath.eval("access_token", map), instanceOf(String.class));
        assertThat(ObjectPath.eval("refresh_token", map), instanceOf(String.class));
        try (RestClient accessTokenClient = restClientWithToken(ObjectPath.eval("access_token", map))) {
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

    private String encode(String param) {
        return URLEncoder.encode(param, StandardCharsets.UTF_8);
    }

    private Long asLong(Object val) {
        if (val == null) {
            return null;
        }
        if (val instanceof Long) {
            return (Long) val;
        }
        if (val instanceof Number) {
            return ((Number) val).longValue();
        }
        if (val instanceof String) {
            return Long.parseLong((String) val);
        }
        throw new IllegalArgumentException("Value [" + val + "] of type [" + val.getClass() + "] is not a Long");
    }

    private RestClient restClientWithToken(String accessToken) throws IOException {
        return buildClient(
            Settings.builder().put(ThreadContext.PREFIX + ".Authorization", "Bearer "+accessToken).build(),
            getClusterHosts().toArray(new HttpHost[getClusterHosts().size()]));
    }
}

