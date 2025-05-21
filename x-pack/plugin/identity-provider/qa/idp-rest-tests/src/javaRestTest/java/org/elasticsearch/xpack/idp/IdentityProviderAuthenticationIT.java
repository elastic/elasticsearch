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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.action.saml.SamlPrepareAuthenticationResponse;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex;
import org.junit.Before;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class IdentityProviderAuthenticationIT extends IdpRestTestCase {

    // From build.gradle
    private final String SP_ENTITY_ID = "ec:123456:abcdefg";
    private final String SP_ACS = "https://sp1.test.es.elasticsearch.org/saml/acs";
    private final String REALM_NAME = "cloud-saml";

    @Before
    public void setupSecurityData() throws IOException {
        setUserPassword("kibana_system", new SecureString("kibana_system".toCharArray()));
        createApplicationPrivileges(
            "elastic-cloud",
            Map.ofEntries(Map.entry("deployment_admin", Set.of("sso:admin")), Map.entry("deployment_viewer", Set.of("sso:viewer")))
        );
    }

    public void testRegistrationAndIdpInitiatedSso() throws Exception {
        final Map<String, Object> request = Map.ofEntries(
            Map.entry("name", "Test SP"),
            Map.entry("acs", SP_ACS),
            Map.entry("privileges", Map.ofEntries(Map.entry("resource", SP_ENTITY_ID), Map.entry("roles", List.of("sso:(\\w+)")))),
            Map.entry(
                "attributes",
                Map.ofEntries(
                    Map.entry("principal", "https://idp.test.es.elasticsearch.org/attribute/principal"),
                    Map.entry("name", "https://idp.test.es.elasticsearch.org/attribute/name"),
                    Map.entry("email", "https://idp.test.es.elasticsearch.org/attribute/email"),
                    Map.entry("roles", "https://idp.test.es.elasticsearch.org/attribute/roles")
                )
            )
        );
        final SamlServiceProviderIndex.DocumentVersion docVersion = createServiceProvider(SP_ENTITY_ID, request);
        checkIndexDoc(docVersion);
        ensureGreen(SamlServiceProviderIndex.INDEX_NAME);
        final String samlResponse = generateSamlResponse(SP_ENTITY_ID, SP_ACS, null);
        authenticateWithSamlResponse(samlResponse, null);
    }

    public void testCustomAttributesInIdpInitiatedSso() throws Exception {
        final Map<String, Object> request = Map.ofEntries(
            Map.entry("name", "Test SP With Custom Attributes"),
            Map.entry("acs", SP_ACS),
            Map.entry("privileges", Map.ofEntries(Map.entry("resource", SP_ENTITY_ID), Map.entry("roles", List.of("sso:(\\w+)")))),
            Map.entry(
                "attributes",
                Map.ofEntries(
                    Map.entry("principal", "https://idp.test.es.elasticsearch.org/attribute/principal"),
                    Map.entry("name", "https://idp.test.es.elasticsearch.org/attribute/name"),
                    Map.entry("email", "https://idp.test.es.elasticsearch.org/attribute/email"),
                    Map.entry("roles", "https://idp.test.es.elasticsearch.org/attribute/roles")
                )
            )
        );
        final SamlServiceProviderIndex.DocumentVersion docVersion = createServiceProvider(SP_ENTITY_ID, request);
        checkIndexDoc(docVersion);
        ensureGreen(SamlServiceProviderIndex.INDEX_NAME);

        // Create custom attributes
        Map<String, List<String>> attributesMap = Map.of("department", List.of("engineering", "product"), "region", List.of("APJ"));

        // Generate SAML response with custom attributes
        final String samlResponse = generateSamlResponseWithAttributes(SP_ENTITY_ID, SP_ACS, null, attributesMap);

        // Parse XML directly from samlResponse (it's not base64 encoded at this point)
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true); // Required for XPath
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(new InputSource(new StringReader(samlResponse)));

        // Create XPath evaluator
        XPathFactory xPathFactory = XPathFactory.newInstance();
        XPath xpath = xPathFactory.newXPath();

        // Validate SAML Response structure
        Element responseElement = (Element) xpath.evaluate("//*[local-name()='Response']", document, XPathConstants.NODE);
        assertThat("SAML Response element should exist", responseElement, notNullValue());

        Element assertionElement = (Element) xpath.evaluate("//*[local-name()='Assertion']", document, XPathConstants.NODE);
        assertThat("SAML Assertion element should exist", assertionElement, notNullValue());

        // Validate department attribute
        NodeList departmentAttributes = (NodeList) xpath.evaluate(
            "//*[local-name()='Attribute' and @Name='department']/*[local-name()='AttributeValue']",
            document,
            XPathConstants.NODESET
        );

        assertThat("Should have two values for department attribute", departmentAttributes.getLength(), is(2));

        // Verify department values
        List<String> departmentValues = new ArrayList<>();
        for (int i = 0; i < departmentAttributes.getLength(); i++) {
            departmentValues.add(departmentAttributes.item(i).getTextContent());
        }
        assertThat(
            "Department attribute should contain 'engineering' and 'product'",
            departmentValues,
            containsInAnyOrder("engineering", "product")
        );

        // Validate region attribute
        NodeList regionAttributes = (NodeList) xpath.evaluate(
            "//*[local-name()='Attribute' and @Name='region']/*[local-name()='AttributeValue']",
            document,
            XPathConstants.NODESET
        );

        assertThat("Should have one value for region attribute", regionAttributes.getLength(), is(1));
        assertThat("Region attribute should contain 'APJ'", regionAttributes.item(0).getTextContent(), equalTo("APJ"));

        authenticateWithSamlResponse(samlResponse, null);
    }

    public void testRegistrationAndSpInitiatedSso() throws Exception {
        final Map<String, Object> request = Map.ofEntries(
            Map.entry("name", "Test SP"),
            Map.entry("acs", SP_ACS),
            Map.entry("privileges", Map.ofEntries(Map.entry("resource", SP_ENTITY_ID), Map.entry("roles", List.of("sso:(\\w+)")))),
            Map.entry(
                "attributes",
                Map.ofEntries(
                    Map.entry("principal", "https://idp.test.es.elasticsearch.org/attribute/principal"),
                    Map.entry("name", "https://idp.test.es.elasticsearch.org/attribute/name"),
                    Map.entry("email", "https://idp.test.es.elasticsearch.org/attribute/email"),
                    Map.entry("roles", "https://idp.test.es.elasticsearch.org/attribute/roles")
                )
            )
        );
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

    private String generateSamlResponse(String entityId, String acs, @Nullable Map<String, Object> authnState) throws IOException {
        return generateSamlResponseWithAttributes(entityId, acs, authnState, null);
    }

    private String generateSamlResponseWithAttributes(
        String entityId,
        String acs,
        @Nullable Map<String, Object> authnState,
        @Nullable Map<String, List<String>> attributes
    ) throws IOException {
        final Request request = new Request("POST", "/_idp/saml/init");

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.field("entity_id", entityId);
        builder.field("acs", acs);

        if (authnState != null) {
            builder.field("authn_state");
            builder.map(authnState);
        }

        if (attributes != null) {
            builder.field("attributes");
            builder.map(attributes);
        }

        builder.endObject();
        String jsonEntity = Strings.toString(builder);

        request.setJsonEntity(jsonEntity);
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("es-secondary-authorization", basicAuthHeaderValue("idp_user", new SecureString("idp-password".toCharArray())))
                .build()
        );
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
            request.setJsonEntity(Strings.format("""
                {"content":"%s", "realm":"%s", "ids":["%s"]}
                """, encodedResponse, REALM_NAME, id));
        } else {
            request.setJsonEntity(Strings.format("""
                {"content":"%s", "realm":"%s"}
                """, encodedResponse, REALM_NAME));
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
            assertThat(
                ObjectPath.eval("metadata.saml_nameid_format", authMap),
                equalTo("urn:oasis:names:tc:SAML:2.0:nameid-format:transient")
            );
            assertThat(ObjectPath.eval("metadata.saml_roles", authMap), instanceOf(List.class));
            assertThat(ObjectPath.eval("metadata.saml_roles", authMap), hasSize(2));
            assertThat(ObjectPath.eval("metadata.saml_roles", authMap), containsInAnyOrder("viewer", "custom"));
        }
    }

    private RestClient restClientWithToken(String accessToken) throws IOException {
        return buildClient(
            Settings.builder().put(ThreadContext.PREFIX + ".Authorization", "Bearer " + accessToken).build(),
            getClusterHosts().toArray(new HttpHost[getClusterHosts().size()])
        );
    }

    private RestClient restClientAsKibanaSystem() throws IOException {
        return buildClient(
            Settings.builder()
                .put(
                    ThreadContext.PREFIX + ".Authorization",
                    basicAuthHeaderValue("kibana_system", new SecureString("kibana_system".toCharArray()))
                )
                .build(),
            getClusterHosts().toArray(new HttpHost[getClusterHosts().size()])
        );
    }
}
