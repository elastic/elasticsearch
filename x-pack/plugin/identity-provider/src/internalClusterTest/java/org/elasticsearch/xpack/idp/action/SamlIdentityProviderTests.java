/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderDocument;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.test.IdentityProviderIntegTestCase;
import org.hamcrest.Matchers;
import org.opensaml.core.xml.util.XMLObjectSupport;
import org.opensaml.saml.common.SAMLObject;
import org.opensaml.saml.saml2.core.AuthnRequest;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.NameIDPolicy;
import org.opensaml.security.SecurityException;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.xmlsec.crypto.XMLSigningUtil;
import org.opensaml.xmlsec.signature.support.SignatureConstants;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numClientNodes = 0, numDataNodes = 0)
public class SamlIdentityProviderTests extends IdentityProviderIntegTestCase {

    private final SamlFactory samlFactory = new SamlFactory();

    public void testIdpInitiatedSso() throws Exception {
        String acsUrl = "https://" + randomAlphaOfLength(12) + ".elastic-cloud.com/saml/acs";
        String entityId = SP_ENTITY_ID;
        setupTestData(entityId, acsUrl);

        ObjectPath objectPath = performSso(entityId, acsUrl, SAMPLE_IDPUSER_NAME, new SecureString(SAMPLE_IDPUSER_PASSWORD.toCharArray()));
        assertThat(objectPath.evaluate("post_url").toString(), equalTo(acsUrl));
        assertSamlResponseForServiceProvider(objectPath, entityId, acsUrl);
        assertSamlResponseUserData(objectPath, SAMPLE_IDPUSER_NAME, "superuser");
    }

    public void testIdpInitiatedSsoWithMultipleRoles() throws Exception {
        final String acsUrl = "https://" + randomAlphaOfLength(12) + ".elastic-cloud.com/saml/acs";
        final String entityId = SP_ENTITY_ID;

        setupTestData(entityId, acsUrl);

        final String entityWildcard = entityId.substring(0, entityId.lastIndexOf(':')) + "*";
        final String username = "user_" + randomAlphaOfLength(5);
        final SecureString password = new SecureString(randomAlphaOfLength(8).toCharArray());
        final String roleName = "role_" + username;
        final RequestOptions adminOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader(
                "Authorization",
                UsernamePasswordToken.basicAuthHeaderValue(SAMPLE_USER_NAME, new SecureString(SAMPLE_USER_PASSWORD.toCharArray()))
            )
            .build();
        // This role has "editor" on the deployment itself, and "viewer" for the organization that owns the deployment
        createRole(roleName, Strings.format("""
            {
              "cluster": [ "manage_own_api_key" ],
              "applications": [
                {
                  "application": "elastic-cloud",
                  "resources": [ "%s" ],
                  "privileges": [ "sso:editor" ]
                },
                {
                  "application": "elastic-cloud",
                  "resources": [ "%s" ],
                  "privileges": [ "sso:viewer" ]
                }
              ]
            }
            """, SP_ENTITY_ID, entityWildcard), adminOptions);
        createUser(username, password, roleName, adminOptions);

        ObjectPath objectPath = performSso(entityId, acsUrl, username, password);
        assertThat(objectPath.evaluate("post_url").toString(), equalTo(acsUrl));
        assertSamlResponseForServiceProvider(objectPath, entityId, acsUrl);
        assertSamlResponseUserData(objectPath, username, "editor", "viewer");
    }

    public void testIdPInitiatedSsoFailsForUnknownSP() throws Exception {
        String acsUrl = "https://" + randomAlphaOfLength(12) + ".elastic-cloud.com/saml/acs";
        String entityId = SP_ENTITY_ID;
        setupTestData(entityId, acsUrl);
        // User login a.k.a exchange the user credentials for an API Key
        final String apiKeyCredentials = getApiKeyFromCredentials(
            SAMPLE_IDPUSER_NAME,
            new SecureString(SAMPLE_IDPUSER_PASSWORD.toCharArray())
        );
        // Make a request to init an SSO flow with the API Key as secondary authentication
        Request request = new Request("POST", "/_idp/saml/init");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(CONSOLE_USER_NAME, new SecureString(CONSOLE_USER_PASSWORD.toCharArray())))
                .addHeader("es-secondary-authorization", "ApiKey " + apiKeyCredentials)
                .build()
        );
        request.setJsonEntity("{ \"entity_id\": \"" + entityId + randomAlphaOfLength(3) + "\", \"acs\": \"" + acsUrl + "\" }");
        ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(e.getMessage(), containsString("is not known to this Identity Provider"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.BAD_REQUEST.getStatus()));
    }

    public void testIdPInitiatedSsoFailsWithoutSecondaryAuthentication() throws Exception {
        String acsUrl = "https://" + randomAlphaOfLength(12) + ".elastic-cloud.com/saml/acs";
        String entityId = SP_ENTITY_ID;
        setupTestData(entityId, acsUrl);
        // Make a request to init an SSO flow with the API Key as secondary authentication
        Request request = new Request("POST", "/_idp/saml/init");
        request.setOptions(REQUEST_OPTIONS_AS_CONSOLE_USER);
        request.setJsonEntity("{ \"entity_id\": \"" + entityId + "\", \"acs\": \"" + acsUrl + "\" }");
        ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(e.getMessage(), containsString("Request is missing secondary authentication"));
    }

    public void testSpInitiatedSso() throws Exception {
        String acsUrl = "https://" + randomAlphaOfLength(12) + ".elastic-cloud.com/saml/acs";
        String entityId = SP_ENTITY_ID;
        setupTestData(entityId, acsUrl);
        // Validate incoming authentication request
        Request validateRequest = new Request("POST", "/_idp/saml/validate");
        validateRequest.setOptions(REQUEST_OPTIONS_AS_CONSOLE_USER);
        final String nameIdFormat = TRANSIENT;
        final String relayString = randomBoolean() ? randomAlphaOfLength(8) : null;
        final boolean forceAuthn = true;
        final AuthnRequest authnRequest = buildAuthnRequest(
            entityId,
            new URL(acsUrl),
            new URL("https://idp.org/sso/redirect"),
            nameIdFormat,
            forceAuthn
        );
        final String query = getQueryString(authnRequest, relayString, false, null);
        validateRequest.setJsonEntity("{\"authn_request_query\":\"" + query + "\"}");
        Response validateResponse = getRestClient().performRequest(validateRequest);
        ObjectPath validateResponseObject = ObjectPath.createFromResponse(validateResponse);
        Map<String, String> serviceProvider = validateResponseObject.evaluate("service_provider");
        assertThat(serviceProvider, hasKey("entity_id"));
        assertThat(serviceProvider.get("entity_id"), equalTo(entityId));
        assertThat(serviceProvider, hasKey("acs"));
        assertThat(serviceProvider.get("acs"), equalTo(acsUrl));
        assertThat(validateResponseObject.evaluate("force_authn"), equalTo(forceAuthn));
        Map<String, String> authnState = validateResponseObject.evaluate("authn_state");
        assertThat(authnState, hasKey("nameid_format"));
        assertThat(authnState.get("nameid_format"), equalTo(nameIdFormat));
        assertThat(authnState, hasKey("authn_request_id"));
        final String expectedInResponeTo = authnState.get("authn_request_id");

        // User login a.k.a exchange the user credentials for an API Key
        final String apiKeyCredentials = getApiKeyFromCredentials(
            SAMPLE_IDPUSER_NAME,
            new SecureString(SAMPLE_IDPUSER_PASSWORD.toCharArray())
        );
        // Make a request to init an SSO flow with the API Key as secondary authentication
        Request initRequest = new Request("POST", "/_idp/saml/init");
        initRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(CONSOLE_USER_NAME, new SecureString(CONSOLE_USER_PASSWORD.toCharArray())))
                .addHeader("es-secondary-authorization", "ApiKey " + apiKeyCredentials)
                .build()
        );
        XContentBuilder authnStateBuilder = jsonBuilder();
        authnStateBuilder.map(authnState);
        initRequest.setJsonEntity(Strings.format("""
            {"entity_id":"%s","acs":"%s","authn_state":%s}
            """, entityId, serviceProvider.get("acs"), Strings.toString(authnStateBuilder)));
        Response initResponse = getRestClient().performRequest(initRequest);
        ObjectPath initResponseObject = ObjectPath.createFromResponse(initResponse);
        assertThat(initResponseObject.evaluate("post_url").toString(), equalTo(acsUrl));

        final String body = initResponseObject.evaluate("saml_response").toString();
        assertThat(body, containsString("<saml2p:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\"/>"));
        assertThat(body, containsString("Destination=\"" + acsUrl + "\""));
        assertThat(body, containsString("<saml2:Audience>" + entityId + "</saml2:Audience>"));
        assertThat(body, containsString("<saml2:NameID Format=\"" + nameIdFormat + "\">"));
        assertThat(body, containsString("InResponseTo=\"" + expectedInResponeTo + "\""));
        Map<String, String> sp = initResponseObject.evaluate("service_provider");
        assertThat(sp, hasKey("entity_id"));
        assertThat(sp.get("entity_id"), equalTo(entityId));
        assertContainsAttributeWithValues(body, "principal", SAMPLE_IDPUSER_NAME);
        assertContainsAttributeWithValues(body, "roles", "superuser");
    }

    public void testSpInitiatedSsoFailsForUserWithNoAccess() throws Exception {
        String acsUrl = "https://" + randomAlphaOfLength(12) + ".elastic-cloud.com/saml/acs";
        String entityId = SP_ENTITY_ID;
        registerServiceProvider(entityId, acsUrl);
        ensureGreen(SamlServiceProviderIndex.INDEX_NAME);
        // Validate incoming authentication request
        Request validateRequest = new Request("POST", "/_idp/saml/validate");
        validateRequest.setOptions(REQUEST_OPTIONS_AS_CONSOLE_USER);
        final String nameIdFormat = TRANSIENT;
        final String relayString = randomBoolean() ? randomAlphaOfLength(8) : null;
        final boolean forceAuthn = true;
        final AuthnRequest authnRequest = buildAuthnRequest(
            entityId,
            new URL(acsUrl),
            new URL("https://idp.org/sso/redirect"),
            nameIdFormat,
            forceAuthn
        );
        final String query = getQueryString(authnRequest, relayString, false, null);
        validateRequest.setJsonEntity("{\"authn_request_query\":\"" + query + "\"}");
        Response validateResponse = getRestClient().performRequest(validateRequest);
        ObjectPath validateResponseObject = ObjectPath.createFromResponse(validateResponse);
        Map<String, String> serviceProvider = validateResponseObject.evaluate("service_provider");
        assertThat(serviceProvider, hasKey("entity_id"));
        assertThat(serviceProvider.get("entity_id"), equalTo(entityId));
        assertThat(serviceProvider, hasKey("acs"));
        assertThat(serviceProvider.get("acs"), equalTo(acsUrl));
        assertThat(validateResponseObject.evaluate("force_authn"), equalTo(forceAuthn));
        Map<String, String> authnState = validateResponseObject.evaluate("authn_state");
        assertThat(authnState, hasKey("nameid_format"));
        assertThat(authnState.get("nameid_format"), equalTo(nameIdFormat));
        assertThat(authnState, hasKey("authn_request_id"));
        final String expectedInResponeTo = authnState.get("authn_request_id");

        // User login a.k.a exchange the user credentials for an API Key - user can authenticate but shouldn't have access this SP
        final String apiKeyCredentials = getApiKeyFromCredentials(SAMPLE_USER_NAME, new SecureString(SAMPLE_USER_PASSWORD.toCharArray()));
        // Make a request to init an SSO flow with the API Key as secondary authentication
        Request initRequest = new Request("POST", "/_idp/saml/init");
        initRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(CONSOLE_USER_NAME, new SecureString(CONSOLE_USER_PASSWORD.toCharArray())))
                .addHeader("es-secondary-authorization", "ApiKey " + apiKeyCredentials)
                .build()
        );
        XContentBuilder authnStateBuilder = jsonBuilder();
        authnStateBuilder.map(authnState);
        initRequest.setJsonEntity(Strings.format("""
            {"entity_id":"%s", "acs":"%s","authn_state":%s}
            """, entityId, acsUrl, Strings.toString(authnStateBuilder)));
        Response initResponse = getRestClient().performRequest(initRequest);
        ObjectPath initResponseObject = ObjectPath.createFromResponse(initResponse);
        assertThat(initResponseObject.evaluate("post_url").toString(), equalTo(acsUrl));
        final String body = initResponseObject.evaluate("saml_response").toString();
        assertThat(body, containsString("<saml2p:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Requester\"/>"));
        assertThat(body, containsString("InResponseTo=\"" + expectedInResponeTo + "\""));
        Map<String, String> sp = initResponseObject.evaluate("service_provider");
        assertThat(sp, hasKey("entity_id"));
        assertThat(sp.get("entity_id"), equalTo(entityId));
        assertThat(
            initResponseObject.evaluate("error"),
            equalTo("User [" + SAMPLE_USER_NAME + "] is not permitted to access service [" + entityId + "]")
        );
    }

    public void testSpInitiatedSsoFailsForUnknownSp() throws Exception {
        String acsUrl = "https://" + randomAlphaOfLength(12) + ".elastic-cloud.com/saml/acs";
        String entityId = SP_ENTITY_ID;
        setupTestData(entityId, acsUrl);
        // Validate incoming authentication request
        Request validateRequest = new Request("POST", "/_idp/saml/validate");
        validateRequest.setOptions(REQUEST_OPTIONS_AS_CONSOLE_USER);
        final String nameIdFormat = TRANSIENT;
        final String relayString = null;
        final boolean forceAuthn = randomBoolean();
        final AuthnRequest authnRequest = buildAuthnRequest(
            entityId + randomAlphaOfLength(4),
            new URL(acsUrl),
            new URL("https://idp.org/sso/redirect"),
            nameIdFormat,
            forceAuthn
        );
        final String query = getQueryString(authnRequest, relayString, false, null);
        validateRequest.setJsonEntity("{\"authn_request_query\":\"" + query + "\"}");
        ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(validateRequest));
        assertThat(e.getMessage(), containsString("is not known to this Identity Provider"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.BAD_REQUEST.getStatus()));
    }

    public void testSpInitiatedSsoFailsForMalformedRequest() throws Exception {
        String acsUrl = "https://" + randomAlphaOfLength(12) + ".elastic-cloud.com/saml/acs";
        String entityId = SP_ENTITY_ID;
        setupTestData(entityId, acsUrl);

        // Validate incoming authentication request
        Request validateRequest = new Request("POST", "/_idp/saml/validate");
        validateRequest.setOptions(REQUEST_OPTIONS_AS_CONSOLE_USER);
        final String nameIdFormat = TRANSIENT;
        final String relayString = null;
        final boolean forceAuthn = randomBoolean();
        final AuthnRequest authnRequest = buildAuthnRequest(
            entityId + randomAlphaOfLength(4),
            new URL(acsUrl),
            new URL("https://idp.org/sso/redirect"),
            nameIdFormat,
            forceAuthn
        );
        final String query = getQueryString(authnRequest, relayString, false, null);

        // Skip http parameter name
        final String queryWithoutParam = query.substring(12);
        validateRequest.setJsonEntity("{\"authn_request_query\":\"" + queryWithoutParam + "\"}");
        ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(validateRequest));
        assertThat(e.getMessage(), containsString("does not contain a SAMLRequest parameter"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.BAD_REQUEST.getStatus()));

        // arbitrarily trim the request
        final String malformedRequestQuery = query.substring(0, query.length() - randomIntBetween(10, 15));
        validateRequest.setJsonEntity("{\"authn_request_query\":\"" + malformedRequestQuery + "\"}");
        ResponseException e1 = expectThrows(ResponseException.class, () -> getRestClient().performRequest(validateRequest));
        assertThat(e1.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.BAD_REQUEST.getStatus()));
    }

    private ObjectPath performSso(String entityId, String acsUrl, String username, SecureString password) throws IOException {
        // User login a.k.a exchange the user credentials for an API Key
        final String apiKeyCredentials = getApiKeyFromCredentials(username, password);
        // Make a request to init an SSO flow with the API Key as secondary authentication
        Request request = new Request("POST", "/_idp/saml/init");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(CONSOLE_USER_NAME, new SecureString(CONSOLE_USER_PASSWORD.toCharArray())))
                .addHeader("es-secondary-authorization", "ApiKey " + apiKeyCredentials)
                .build()
        );
        request.setJsonEntity("{ \"entity_id\": \"" + entityId + "\", \"acs\": \"" + acsUrl + "\" }");
        Response initResponse = getRestClient().performRequest(request);
        ObjectPath objectPath = ObjectPath.createFromResponse(initResponse);
        return objectPath;
    }

    private void assertSamlResponseForServiceProvider(ObjectPath objectPath, String entityId, String acsUrl) throws IOException {
        String body = objectPath.evaluate("saml_response").toString();
        assertThat(body, containsString("Destination=\"" + acsUrl + "\""));
        assertThat(body, containsString("<saml2:Audience>" + entityId + "</saml2:Audience>"));
        assertThat(body, containsString("<saml2:NameID Format=\"" + TRANSIENT + "\">"));
        Map<String, String> serviceProvider = objectPath.evaluate("service_provider");
        assertThat(serviceProvider, hasKey("entity_id"));
        assertThat(serviceProvider.get("entity_id"), equalTo(entityId));
    }

    private void assertSamlResponseUserData(ObjectPath objectPath, String idpuserName, String... roles) throws IOException {
        var body = objectPath.evaluate("saml_response").toString();
        assertContainsAttributeWithValues(body, "principal", idpuserName);
        assertContainsAttributeWithValues(body, "roles", roles);
    }

    private void setupTestData(String entityId, String acsUrl) throws Exception {
        registerServiceProvider(entityId, acsUrl);
        registerApplicationPrivileges();
        ensureGreen(SamlServiceProviderIndex.INDEX_NAME);
    }

    private void registerServiceProvider(String entityId, String acsUrl) throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureYellowAndNoInitializingShards();

        Map<String, Object> spFields = new HashMap<>();
        spFields.put(SamlServiceProviderDocument.Fields.ACS.getPreferredName(), acsUrl);
        spFields.put(SamlServiceProviderDocument.Fields.ENTITY_ID.getPreferredName(), entityId);
        spFields.put(SamlServiceProviderDocument.Fields.NAME_ID.getPreferredName(), TRANSIENT);
        spFields.put(SamlServiceProviderDocument.Fields.NAME.getPreferredName(), "Dummy SP");
        spFields.put(
            "attributes",
            Map.of(
                "principal",
                "https://saml.elasticsearch.org/attributes/principal",
                "roles",
                "https://saml.elasticsearch.org/attributes/roles"
            )
        );
        spFields.put("privileges", Map.of("resource", entityId, "roles", Set.of("sso:(\\w+)")));
        Request request = new Request(
            "PUT",
            "/_idp/saml/sp/" + urlEncode(entityId) + "?refresh=" + WriteRequest.RefreshPolicy.IMMEDIATE.getValue()
        );
        request.setOptions(REQUEST_OPTIONS_AS_CONSOLE_USER);
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.map(spFields);
        request.setJsonEntity(Strings.toString(builder));
        Response registerResponse = getRestClient().performRequest(request);
        assertThat(registerResponse.getStatusLine().getStatusCode(), equalTo(200));
        ObjectPath registerResponseObject = ObjectPath.createFromResponse(registerResponse);
        Map<String, Object> document = registerResponseObject.evaluate("document");
        assertThat(document, hasKey("_created"));
        assertThat(document.get("_created"), equalTo(true));
        Map<String, Object> serviceProvider = registerResponseObject.evaluate("service_provider");
        assertThat(serviceProvider, hasKey("entity_id"));
        assertThat(serviceProvider.get("entity_id"), equalTo(entityId));
        assertThat(serviceProvider, hasKey("enabled"));
        assertThat(serviceProvider.get("enabled"), equalTo(true));
    }

    private void registerApplicationPrivileges() throws IOException {
        registerApplicationPrivileges(
            Map.ofEntries(
                Map.entry("deployment_admin", Set.of("sso:superuser")),
                Map.entry("deployment_editor", Set.of("sso:editor")),
                Map.entry("deployment_viewer", Set.of("sso:viewer"))
            )
        );
    }

    private void registerApplicationPrivileges(Map<String, Set<String>> privileges) throws IOException {
        Request request = new Request("PUT", "/_security/privilege?refresh=" + WriteRequest.RefreshPolicy.IMMEDIATE.getValue());
        request.setOptions(REQUEST_OPTIONS_AS_CONSOLE_USER);
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("elastic-cloud"); // app-name
        privileges.forEach((privName, actions) -> {
            try {
                builder.startObject(privName);
                builder.field("actions", actions);
                builder.endObject();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        builder.endObject(); // app-name
        builder.endObject(); // root
        request.setJsonEntity(Strings.toString(builder));

        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }

    private void createRole(String roleName, String roleBody, RequestOptions options) throws IOException {
        Request req = new Request("PUT", "/_security/role/" + roleName);
        req.setJsonEntity(roleBody);
        req.setOptions(options);
        getRestClient().performRequest(req);
    }

    private void createUser(String userName, SecureString password, String roleName, RequestOptions options) throws IOException {
        final Request req = new Request("PUT", "/_security/user/" + userName);
        final String body = Strings.format("""
            {
              "username": "%s",
              "full_name": "Test User (%s)",
              "password": "%s",
              "roles": [ "%s" ]
            }
            """, userName, getTestName(), password, roleName);
        req.setJsonEntity(body);
        req.setOptions(options);
        getRestClient().performRequest(req);
    }

    private String getApiKeyFromCredentials(String username, SecureString password) {
        Client client = client().filterWithHeader(
            Collections.singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue(username, password))
        );
        final CreateApiKeyResponse response = new CreateApiKeyRequestBuilder(client).setName("test key")
            .setExpiration(TimeValue.timeValueHours(TimeUnit.DAYS.toHours(7L)))
            .get();
        assertNotNull(response);
        return Base64.getEncoder().encodeToString((response.getId() + ":" + response.getKey().toString()).getBytes(StandardCharsets.UTF_8));
    }

    private AuthnRequest buildAuthnRequest(String entityId, URL acs, URL destination, String nameIdFormat, boolean forceAuthn) {
        final Issuer issuer = samlFactory.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(entityId);
        final NameIDPolicy nameIDPolicy = samlFactory.buildObject(NameIDPolicy.class, NameIDPolicy.DEFAULT_ELEMENT_NAME);
        nameIDPolicy.setFormat(nameIdFormat);
        final AuthnRequest authnRequest = samlFactory.buildObject(AuthnRequest.class, AuthnRequest.DEFAULT_ELEMENT_NAME);
        authnRequest.setID(samlFactory.secureIdentifier());
        authnRequest.setIssuer(issuer);
        authnRequest.setIssueInstant(Instant.now());
        authnRequest.setAssertionConsumerServiceURL(acs.toString());
        authnRequest.setDestination(destination.toString());
        authnRequest.setNameIDPolicy(nameIDPolicy);
        authnRequest.setForceAuthn(forceAuthn);
        return authnRequest;
    }

    private String getQueryString(AuthnRequest authnRequest, String relayState, boolean sign, @Nullable X509Credential credential) {
        try {
            final String request = deflateAndBase64Encode(authnRequest);
            String queryParam = "SAMLRequest=" + urlEncode(request);
            if (relayState != null) {
                queryParam += "&RelayState=" + urlEncode(relayState);
            }
            if (sign) {
                final String algo = SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA256;
                queryParam += "&SigAlg=" + urlEncode(algo);
                final byte[] sig = sign(queryParam, algo, credential);
                queryParam += "&Signature=" + urlEncode(base64Encode(sig));
            }
            return queryParam;
        } catch (Exception e) {
            throw new ElasticsearchException("Cannot construct SAML redirect", e);
        }
    }

    private String base64Encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static String urlEncode(String param) {
        return URLEncoder.encode(param, StandardCharsets.UTF_8);
    }

    private String deflateAndBase64Encode(SAMLObject message) throws Exception {
        Deflater deflater = new Deflater(Deflater.DEFLATED, true);
        try (
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            DeflaterOutputStream deflaterStream = new DeflaterOutputStream(bytesOut, deflater)
        ) {
            String messageStr = samlFactory.toString(XMLObjectSupport.marshall(message), false);
            deflaterStream.write(messageStr.getBytes(StandardCharsets.UTF_8));
            deflaterStream.finish();
            return base64Encode(bytesOut.toByteArray());
        }
    }

    private byte[] sign(String text, String algo, X509Credential credential) throws SecurityException {
        return sign(text.getBytes(StandardCharsets.UTF_8), algo, credential);
    }

    private byte[] sign(byte[] content, String algo, X509Credential credential) throws SecurityException {
        return XMLSigningUtil.signWithURI(credential, algo, content);
    }

    private void assertContainsAttributeWithValues(String message, String attribute, String... values) {
        final String startAttribute = Strings.format("""
            <saml2:Attribute FriendlyName="%s" Name="https://saml.elasticsearch.org/attributes/%s" \
            NameFormat="urn:oasis:names:tc:SAML:2.0:attrname-format:uri">""", attribute, attribute);
        assertThat(message, containsString(startAttribute));
        final int posStart = message.indexOf(startAttribute);
        assertThat(posStart, Matchers.greaterThan(0));

        final String endAttribute = "</saml2:Attribute>";
        assertThat(message, containsString(endAttribute));
        final int posEnd = message.indexOf(endAttribute, posStart);
        assertThat(posEnd, Matchers.greaterThan(posStart));

        final String attributeContent = message.substring(posStart + startAttribute.length(), posEnd);

        for (String value : values) {
            assertThat(
                attributeContent,
                containsString(
                    "<saml2:AttributeValue xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xsd:string\">"
                        + value
                        + "</saml2:AttributeValue>"
                )
            );
        }
    }
}
