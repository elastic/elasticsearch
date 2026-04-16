/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.core.Strings;
import org.hamcrest.Matcher;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.LogoutRequest;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.security.x509.X509Credential;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SamlRedirectTests extends SamlTestCase {

    private static final String IDP_ENTITY_ID = "https://idp.test/";
    private static final String SP_ENTITY_ID = "https://sp.example.com/";
    private static final String ACS_URL = "https://sp.example.com/saml/acs";
    private static final String IDP_URL = "https://idp.test/saml/sso/redirect";
    private static final String LOGOUT_URL = "https://idp.test/saml/logout";

    private static final SigningConfiguration NO_SIGNING = new SigningConfiguration(emptySet(), null);

    /**
     * XML template of LogoutRequest.
     */
    private static final String EXPECTED_LOGOUT_REQUEST_TEMPLATE = """
        <?xml version="1.0" encoding="UTF-8"?>\
        <saml2p:LogoutRequest xmlns:saml2p="urn:oasis:names:tc:SAML:2.0:protocol" \
        Destination="%s" \
        ID="_id123456789" IssueInstant="2018-01-14T22:47:00.000Z" Version="2.0">\
        <saml2:Issuer xmlns:saml2="urn:oasis:names:tc:SAML:2.0:assertion">https://idp.test/</saml2:Issuer>\
        <saml2:NameID xmlns:saml2="urn:oasis:names:tc:SAML:2.0:assertion">name-123456-7890</saml2:NameID>\
        </saml2p:LogoutRequest>""";

    public void testRedirectUrlWithoutRelayStateOrSigning() {
        final SamlRedirect redirect = new SamlRedirect(buildLogoutRequest(LOGOUT_URL), NO_SIGNING);
        final String url = redirect.getRedirectUrl();
        assertRedirectUrl(url, Map.of("SAMLRequest", equalTo(buildExpectedLogoutRequestString(LOGOUT_URL))));
    }

    public void testRedirectUrlWithRelayStateAndSigning() throws Exception {
        final SigningConfiguration signing = new SigningConfiguration(
            singleton("*"),
            (X509Credential) buildOpenSamlCredential(readRandomKeyPair()).get(0)
        );
        final SamlRedirect redirect = new SamlRedirect(buildLogoutRequest(LOGOUT_URL), signing);
        final String url = redirect.getRedirectUrl("hello");
        assertRedirectUrl(
            url,
            Map.of(
                "SAMLRequest",
                equalTo(buildExpectedLogoutRequestString(LOGOUT_URL)),
                "RelayState",
                equalTo("hello"),
                "SigAlg",
                equalTo("http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"),
                "Signature",
                notNullValue(String.class)
            )
        );
    }

    public void testRedirectUrlWithExistingParameters() {
        final SamlRedirect redirect = new SamlRedirect(buildLogoutRequest(LOGOUT_URL + "?a=xyz"), NO_SIGNING);
        final String url = redirect.getRedirectUrl("foo");
        assertRedirectUrl(
            url,
            Map.of(
                "SAMLRequest",
                equalTo(buildExpectedLogoutRequestString(LOGOUT_URL + "?a=xyz")),
                "RelayState",
                equalTo("foo"),
                "a",
                equalTo("xyz")
            )
        );
    }

    public void testRedirectUrlWithTrailingQuestionMark() {
        final SamlRedirect redirect = new SamlRedirect(buildLogoutRequest(LOGOUT_URL + "?"), NO_SIGNING);
        final String url = redirect.getRedirectUrl();
        assertRedirectUrl(url, Map.of("SAMLRequest", equalTo(buildExpectedLogoutRequestString(LOGOUT_URL + "?"))));
    }

    private static String buildExpectedLogoutRequestString(String destination) {
        return Strings.format(EXPECTED_LOGOUT_REQUEST_TEMPLATE, destination);
    }

    private static Map<String, String> parseAndDecodeUrlParameters(String url) {
        return parseAndDecodeUrlParameters(url, (key, value) -> value);
    }

    private static Map<String, String> parseAndDecodeUrlParameters(String url, BiFunction<String, String, String> valueDecoder) {
        Map<String, String> params = new HashMap<>();
        String[] parts = url.split("\\?", 2);
        if (parts.length < 2 || parts[1].isEmpty()) {
            return params;
        }
        for (String param : parts[1].split("&")) {
            String[] keyValue = param.split("=", 2);
            if (keyValue.length == 2) {
                String key = keyValue[0];
                String value = URLDecoder.decode(keyValue[1], StandardCharsets.UTF_8);
                params.put(key, valueDecoder.apply(key, value));
            }
        }
        return params;
    }

    private void assertRedirectUrl(String actualRequestUrl, Map<String, Matcher<String>> expectedParams) {
        String[] parts = actualRequestUrl.split("\\?", 2);
        assertThat(parts[0], equalTo(LOGOUT_URL));

        Map<String, String> actualParams = parseAndDecodeUrlParameters(actualRequestUrl, (key, value) -> {
            if (key.equals("SAMLRequest")) {
                return decompressAndBase64Decode(value);
            }
            return value;
        });
        assertThat("URL parameter keys", actualParams.keySet(), equalTo(expectedParams.keySet()));

        for (Map.Entry<String, Matcher<String>> expected : expectedParams.entrySet()) {
            assertThat("Parameter " + expected.getKey(), actualParams.get(expected.getKey()), expected.getValue());
        }
    }

    private static String decompressAndBase64Decode(String compressedBase64) {
        byte[] compressed = Base64.getDecoder().decode(compressedBase64);
        Inflater inflater = new Inflater(true);
        try (InflaterInputStream in = new InflaterInputStream(new ByteArrayInputStream(compressed), inflater)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to decompress input: " + compressedBase64, e);
        }
    }

    public void testLogoutRequestSigning() throws Exception {
        final X509Credential credential = (X509Credential) buildOpenSamlCredential(readRandomKeyPair()).get(0);
        X509Credential invalidCredential = (X509Credential) buildOpenSamlCredential(readRandomKeyPair()).get(0);
        while (invalidCredential.getEntityCertificate().getSerialNumber().equals(credential.getEntityCertificate().getSerialNumber())) {
            invalidCredential = (X509Credential) buildOpenSamlCredential(readRandomKeyPair()).get(0);
        }
        final SigningConfiguration spConfig = new SigningConfiguration(singleton("*"), credential);
        final SamlRedirect redirect = new SamlRedirect(buildLogoutRequest(LOGOUT_URL + "?"), spConfig);
        final String url = redirect.getRedirectUrl();
        final String queryParam = url.split("\\?")[1].split("&Signature")[0];
        final String signature = validateUrlAndGetSignature(redirect.getRedirectUrl());
        assertThat(validateSignature(queryParam, signature, credential), equalTo(true));
        assertThat(validateSignature(queryParam, signature, invalidCredential), equalTo(false));
        assertThat(validateSignature(queryParam.substring(0, queryParam.length() - 5), signature, credential), equalTo(false));
    }

    public void testAuthnRequestSigning() throws Exception {
        final X509Credential credential = (X509Credential) buildOpenSamlCredential(readRandomKeyPair()).get(0);
        X509Credential invalidCredential = (X509Credential) buildOpenSamlCredential(readRandomKeyPair()).get(0);
        while (invalidCredential.getEntityCertificate().getSerialNumber().equals(credential.getEntityCertificate().getSerialNumber())) {
            invalidCredential = (X509Credential) buildOpenSamlCredential(readRandomKeyPair()).get(0);
        }
        final SigningConfiguration signingConfig = new SigningConfiguration(singleton("*"), credential);
        SpConfiguration sp = new SingleSamlSpConfiguration(SP_ENTITY_ID, ACS_URL, LOGOUT_URL, signingConfig, null, Collections.emptyList());

        EntityDescriptor idpDescriptor = buildIdPDescriptor(IDP_URL, IDP_ENTITY_ID);

        final SamlRedirect redirect = new SamlRedirect(
            new SamlAuthnRequestBuilder(
                sp,
                SAMLConstants.SAML2_POST_BINDING_URI,
                idpDescriptor,
                SAMLConstants.SAML2_REDIRECT_BINDING_URI,
                Clock.systemUTC()
            ).build(),
            signingConfig
        );
        final String url = redirect.getRedirectUrl();
        final String queryParam = url.split("\\?")[1].split("&Signature")[0];
        final String signature = validateUrlAndGetSignature(redirect.getRedirectUrl());
        assertThat(validateSignature(queryParam, signature, credential), equalTo(true));
        assertThat(validateSignature(queryParam, signature, invalidCredential), equalTo(false));
        assertThat(validateSignature(queryParam.substring(0, queryParam.length() - 5), signature, credential), equalTo(false));
    }

    private String validateUrlAndGetSignature(String url) {
        Map<String, String> params = parseAndDecodeUrlParameters(url);
        // We currently only support signing with SHA256withRSA, this test should be updated if we add support for more
        assertThat(params.get("SigAlg"), equalTo("http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"));
        return params.get("Signature");
    }

    private boolean validateSignature(String queryParam, String signature, X509Credential credential) {
        try {
            // We currently only support signing with SHA256withRSA, this test should be updated if we add support for more
            Signature sig = Signature.getInstance("SHA256withRSA");
            sig.initVerify(credential.getPublicKey());
            sig.update(queryParam.getBytes(StandardCharsets.UTF_8));
            return sig.verify(Base64.getDecoder().decode(signature));
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            return false;
        }
    }

    private LogoutRequest buildLogoutRequest(String logoutUrl) {
        final LogoutRequest logoutRequest = SamlUtils.buildObject(LogoutRequest.class, LogoutRequest.DEFAULT_ELEMENT_NAME);
        logoutRequest.setDestination(logoutUrl);
        logoutRequest.setIssueInstant(ZonedDateTime.of(LocalDate.of(2018, 1, 14), LocalTime.of(22, 47), ZoneOffset.UTC).toInstant());
        logoutRequest.setID("_id123456789");
        final Issuer issuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(IDP_ENTITY_ID);
        logoutRequest.setIssuer(issuer);
        final NameID nameId = SamlUtils.buildObject(NameID.class, NameID.DEFAULT_ELEMENT_NAME);
        nameId.setValue("name-123456-7890");
        logoutRequest.setNameID(nameId);
        return logoutRequest;
    }

}
