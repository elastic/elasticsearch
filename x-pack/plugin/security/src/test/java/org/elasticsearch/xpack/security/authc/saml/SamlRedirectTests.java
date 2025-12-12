/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class SamlRedirectTests extends SamlTestCase {

    private static final String IDP_ENTITY_ID = "https://idp.test/";
    private static final String SP_ENTITY_ID = "https://sp.example.com/";
    private static final String ACS_URL = "https://sp.example.com/saml/acs";
    private static final String IDP_URL = "https://idp.test/saml/sso/redirect";
    private static final String LOGOUT_URL = "https://idp.test/saml/logout";

    private static final SigningConfiguration NO_SIGNING = new SigningConfiguration(emptySet(), null);

    public void testRedirectUrlWithoutRelayStateOrSigning() {
        final SamlRedirect redirect = new SamlRedirect(buildLogoutRequest(LOGOUT_URL), NO_SIGNING);
        final String url = redirect.getRedirectUrl();
        assertRedirectUrl(
            url,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<saml2p:LogoutRequest xmlns:saml2p=\"urn:oasis:names:tc:SAML:2.0:protocol\" "
                + "Destination=\"https://idp.test/saml/logout\" "
                + "ID=\"_id123456789\" IssueInstant=\"2018-01-14T22:47:00.000Z\" Version=\"2.0\">"
                + "<saml2:Issuer xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\">https://idp.test/</saml2:Issuer>"
                + "<saml2:NameID xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\">name-123456-7890</saml2:NameID>"
                + "</saml2p:LogoutRequest>",
            emptyMap(),
            false
        );
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
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<saml2p:LogoutRequest xmlns:saml2p=\"urn:oasis:names:tc:SAML:2.0:protocol\" "
                + "Destination=\"https://idp.test/saml/logout\" "
                + "ID=\"_id123456789\" IssueInstant=\"2018-01-14T22:47:00.000Z\" Version=\"2.0\">"
                + "<saml2:Issuer xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\">https://idp.test/</saml2:Issuer>"
                + "<saml2:NameID xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\">name-123456-7890</saml2:NameID>"
                + "</saml2p:LogoutRequest>",
            Map.of("RelayState", "hello", "SigAlg", "http%3A%2F%2Fwww.w3.org%2F2001%2F04%2Fxmldsig-more%23rsa-sha256"),
            true
        );
    }

    public void testRedirectUrlWithExistingParameters() {
        final SamlRedirect redirect = new SamlRedirect(buildLogoutRequest(LOGOUT_URL + "?a=xyz"), NO_SIGNING);
        final String url = redirect.getRedirectUrl("foo");
        assertRedirectUrl(
            url,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<saml2p:LogoutRequest xmlns:saml2p=\"urn:oasis:names:tc:SAML:2.0:protocol\" "
                + "Destination=\"https://idp.test/saml/logout?a=xyz\" "
                + "ID=\"_id123456789\" IssueInstant=\"2018-01-14T22:47:00.000Z\" Version=\"2.0\">"
                + "<saml2:Issuer xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\">https://idp.test/</saml2:Issuer>"
                + "<saml2:NameID xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\">name-123456-7890</saml2:NameID>"
                + "</saml2p:LogoutRequest>",
            Map.of("RelayState", "foo", "a", "xyz"),
            false
        );
    }

    public void testRedirectUrlWithTrailingQuestionMark() {
        final SamlRedirect redirect = new SamlRedirect(buildLogoutRequest(LOGOUT_URL + "?"), NO_SIGNING);
        final String url = redirect.getRedirectUrl();
        assertRedirectUrl(
            url,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<saml2p:LogoutRequest xmlns:saml2p=\"urn:oasis:names:tc:SAML:2.0:protocol\" "
                + "Destination=\"https://idp.test/saml/logout?\" "
                + "ID=\"_id123456789\" IssueInstant=\"2018-01-14T22:47:00.000Z\" Version=\"2.0\">"
                + "<saml2:Issuer xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\">https://idp.test/</saml2:Issuer>"
                + "<saml2:NameID xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\">name-123456-7890</saml2:NameID>"
                + "</saml2p:LogoutRequest>",
            emptyMap(),
            false
        );
    }

    private void assertRedirectUrl(
        String actualRequestUrl,
        String expectedUncompressedSamlRequestContent,
        Map<String, String> expectedParams,
        boolean allowExtraParams
    ) {
        Set<String> requiredParams = new HashSet<>(expectedParams.keySet());
        String[] parts = actualRequestUrl.split("\\?");
        assertThat(parts[0], equalTo(LOGOUT_URL));
        final String[] params = parts[1].split("&");
        for (String param : params) {
            final String[] keyValue = param.split("=", 2);
            assertThat(keyValue.length, equalTo(2));
            final String actualKey = keyValue[0];
            final String actualValue = keyValue[1];
            if (actualKey.equals("SAMLRequest")) {
                final String samlRequest = URLDecoder.decode(actualValue, StandardCharsets.UTF_8);
                final String decompressed = decompressAndBase64Decode(samlRequest);
                assertThat(decompressed, equalTo(expectedUncompressedSamlRequestContent));
            } else {
                if (expectedParams.containsKey(actualKey)) {
                    assertThat("invalid value for key " + actualKey, actualValue, equalTo(expectedParams.get(actualKey)));
                    requiredParams.remove(actualKey);
                } else if (allowExtraParams == false) {
                    fail("unexpected parameter: " + actualKey);
                }
            }
        }
        assertThat(requiredParams, empty());
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

    private String parseAndUrlDecodeParameter(String parameter) {
        final String value = parameter.split("=", 2)[1];
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private String validateUrlAndGetSignature(String url) {
        final String params[] = url.split("\\?")[1].split("&");
        assert (params.length == 3);
        String sigAlg = parseAndUrlDecodeParameter(params[1]);
        // We currently only support signing with SHA256withRSA, this test should be updated if we add support for more
        assertThat(sigAlg, equalTo("http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"));
        return parseAndUrlDecodeParameter(params[2]);
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
