/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.action;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.idp.saml.test.IdentityProviderIntegTestCase;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.junit.Before;
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
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.joda.time.DateTime.now;
import static org.opensaml.saml.saml2.core.NameIDType.PERSISTENT;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

public class SamlIdentityProviderIntegTests extends IdentityProviderIntegTestCase {

    private final SamlFactory samlFactory = new SamlFactory();

    @Before
    public void registerServiceProvider() {
        //Do nothing now but https://github.com/elastic/elasticsearch/pull/52936 will probably be merged
        //before this, so register an SP to use for the tests instead of using the static mock one.
    }

    public void testIdpInitiatedSso() throws Exception {
        // User login a.k.a exchange the user credentials for an API Key
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(TEST_USER_NAME,
                new SecureString(TEST_PASSWORD.toCharArray()))));
        final CreateApiKeyResponse response = new CreateApiKeyRequestBuilder(client)
            .setName("test key")
            .setExpiration(TimeValue.timeValueHours(TimeUnit.DAYS.toHours(7L)))
            .get();
        assertNotNull(response);
        final String apiKeyCredentials = Base64.getEncoder().encodeToString(
            (response.getId() + ":" + response.getKey().toString()).getBytes(StandardCharsets.UTF_8));
        // Make a request to init an SSO flow with the API Key as secondary authentication
        Request request = new Request("POST", "/_idp/saml/init");
        request.setOptions(RequestOptions.DEFAULT.toBuilder()
            .addHeader("Authorization", basicAuthHeaderValue(TEST_SUPERUSER,
                new SecureString(TEST_PASSWORD.toCharArray())))
            .addHeader("es-secondary-authorization", "ApiKey " + apiKeyCredentials)
            .build());
        final StringEntity entity = new StringEntity("{ \"sp_entity_id\":\"https://sp.some.org\"}", ContentType.APPLICATION_JSON);
        request.setEntity(entity);
        Response initResponse = getRestClient().performRequest(request);
        ObjectPath objectPath = ObjectPath.createFromResponse(initResponse);
        assertThat(objectPath.evaluate("redirect_url").toString(), equalTo("https://sp.some.org/api/security/v1/saml"));
        final String body = objectPath.evaluate("response_body").toString();
        assertThat(body, containsString("Destination=\"https://sp.some.org/api/security/v1/saml\""));
        assertThat(body, containsString("<saml2:Audience>https://sp.some.org</saml2:Audience>"));
        // NameID
        assertThat(body, containsString(TEST_USER_NAME));
        Map<String, String> serviceProvider = objectPath.evaluate("service_provider");
        assertThat(serviceProvider, hasKey("entity_id"));
        assertThat(serviceProvider.get("entity_id"), equalTo("https://sp.some.org"));

    }

    public void testSpInititatedSso() throws Exception {
        // Validate incoming authentication request
        Request validateRequest = new Request("POST", "/_idp/saml/validate");
        validateRequest.setOptions(IDP_REQUEST_OPTIONS);
        final String nameIdFormat = randomFrom(TRANSIENT, PERSISTENT);
        final String relayString = randomBoolean() ? randomAlphaOfLengthBetween(5, 12) : null;
        final boolean forceAuthn = randomBoolean();
        final AuthnRequest authnRequest = buildAuthnRequest("https://sp.some.org", new URL("https://sp.some.org/api/security/v1/saml"),
            new URL("https://idp.org/sso/redirect"), nameIdFormat, forceAuthn);
        final String query = getQueryString(authnRequest, relayString, false, null);
        StringEntity entity = new StringEntity("{\"authn_request_query\":\"" + query + "\"}", ContentType.APPLICATION_JSON);
        validateRequest.setEntity(entity);
        Response validateResponse = getRestClient().performRequest(validateRequest);
        ObjectPath objectPath = ObjectPath.createFromResponse(validateResponse);
        Map<String, String> serviceProvider = objectPath.evaluate("service_provider");
        assertThat(serviceProvider, hasKey("entity_id"));
        assertThat(serviceProvider.get("entity_id"), equalTo("https://sp.some.org"));
        assertThat(objectPath.evaluate("force_authn"), equalTo(forceAuthn));
        Map<String, String> authnState = objectPath.evaluate("authn_state");
        assertThat(authnState, hasKey("nameid_format"));
        assertThat(authnState.get("nameid_format"), equalTo(nameIdFormat));

        // User login a.k.a exchange the user credentials for an API Key
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(TEST_USER_NAME,
                new SecureString(TEST_PASSWORD.toCharArray()))));
        final CreateApiKeyResponse response = new CreateApiKeyRequestBuilder(client)
            .setName("test key")
            .setExpiration(TimeValue.timeValueHours(TimeUnit.DAYS.toHours(7L)))
            .get();
        assertNotNull(response);
        final String apiKeyCredentials =
            base64Encode((response.getId() + ":" + response.getKey().toString()).getBytes(StandardCharsets.UTF_8));
        // Make a request to init an SSO flow with the API Key as secondary authentication

        Request initRequest = new Request("POST", "/_idp/saml/init");
        initRequest.setOptions(IDP_REQUEST_WITH_SECONDARY_AUTH_OPTIONS);
        entity = new StringEntity("{ \"sp_entity_id\":\"https://sp.some.org\"}", ContentType.APPLICATION_JSON);
        initRequest.setEntity(entity);
        Response initResponse = getRestClient().performRequest(initRequest);
        objectPath = ObjectPath.createFromResponse(initResponse);
        assertThat(objectPath.evaluate("redirect_url").toString(), equalTo("https://sp.some.org/api/security/v1/saml"));
        final String body = objectPath.evaluate("response_body").toString();
        assertThat(body, containsString("<saml2p:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\"/>"));
        assertThat(body, containsString("Destination=\"https://sp.some.org/api/security/v1/saml\""));
        assertThat(body, containsString("<saml2:Audience>https://sp.some.org</saml2:Audience>"));
        assertThat(body, containsString("<saml2:NameID Format=\"" + nameIdFormat + "\">" + TEST_USER_NAME + "</saml2:NameID>"));
        Map<String, String> sp = objectPath.evaluate("service_provider");
        assertThat(sp, hasKey("entity_id"));
        assertThat(sp.get("entity_id"), equalTo("https://sp.some.org"));

    }

    private AuthnRequest buildAuthnRequest(String entityId, URL acs, URL destination, String nameIdFormat, boolean forceAuthn) {
        final Issuer issuer = samlFactory.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(entityId);
        final NameIDPolicy nameIDPolicy = samlFactory.buildObject(NameIDPolicy.class, NameIDPolicy.DEFAULT_ELEMENT_NAME);
        nameIDPolicy.setFormat(nameIdFormat);
        final AuthnRequest authnRequest = samlFactory.buildObject(AuthnRequest.class, AuthnRequest.DEFAULT_ELEMENT_NAME);
        authnRequest.setID(samlFactory.secureIdentifier());
        authnRequest.setIssuer(issuer);
        authnRequest.setIssueInstant(now());
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

    private String urlEncode(String param) throws UnsupportedEncodingException {
        return URLEncoder.encode(param, StandardCharsets.UTF_8.name());
    }

    private String deflateAndBase64Encode(SAMLObject message)
        throws Exception {
        Deflater deflater = new Deflater(Deflater.DEFLATED, true);
        try (ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
             DeflaterOutputStream deflaterStream = new DeflaterOutputStream(bytesOut, deflater)) {
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
}
