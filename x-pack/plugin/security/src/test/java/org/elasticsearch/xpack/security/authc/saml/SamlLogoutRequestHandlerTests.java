/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensaml.saml.common.SAMLObject;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.LogoutRequest;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.xmlsec.signature.support.SignatureConstants;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SamlLogoutRequestHandlerTests extends SamlTestCase {

    private static final String IDP_ENTITY_ID = "https://idp.test/";
    private static final String LOGOUT_URL = "https://sp.test/saml/logout";

    private static X509Credential credential;
    private Clock clock;

    @BeforeClass
    public static void setupCredential() throws Exception {
        credential = (X509Credential) buildOpenSamlCredential(readRandomKeyPair()).get(0);
    }

    @AfterClass
    public static void clearCredential() throws Exception {
        credential = null;
    }

    @Before
    public void setupClock() throws Exception {
        clock = Clock.systemUTC();
    }

    public void testLogoutWithValidSignatureIsParsedSuccessfully() throws Exception {
        final LogoutRequest logoutRequest = buildLogoutRequest();

        final String query = buildSignedQueryString(logoutRequest);

        final SamlLogoutRequestHandler.Result result = buildHandler().parseFromQueryString(query);
        assertResultMatchesRequest(result, logoutRequest);
    }

    private void assertResultMatchesRequest(SamlLogoutRequestHandler.Result result, LogoutRequest logoutRequest) {
        assertThat(result, notNullValue());
        assertThat(result.getNameId(), notNullValue());
        assertThat(result.getNameId().idpNameQualifier, equalTo(logoutRequest.getNameID().getNameQualifier()));
        assertThat(result.getNameId().spNameQualifier, equalTo(logoutRequest.getNameID().getSPNameQualifier()));
        assertThat(result.getNameId().value, equalTo(logoutRequest.getNameID().getValue()));
        assertThat(result.getNameId().spProvidedId, nullValue());
        assertThat(result.getSession(), nullValue());
        assertThat(result.getRequestId(), equalTo(logoutRequest.getID()));
    }

    public void testLogoutWithIncorrectIssuerIsRejected() throws Exception {
        final LogoutRequest logoutRequest = buildLogoutRequest();
        logoutRequest.getIssuer().setValue("https://attacker.bot.net/");

        final String query = buildSignedQueryString(logoutRequest);

        final SamlLogoutRequestHandler handler = buildHandler();
        final ElasticsearchSecurityException exception = expectSamlException(() -> handler.parseFromQueryString(query));
        assertThat(exception.getMessage(), containsString("Issuer"));
        assertThat(exception.getMessage(), containsString(IDP_ENTITY_ID));
        assertThat(exception.getMessage(), containsString(logoutRequest.getIssuer().getValue()));
    }

    public void testLogoutWithIncorrectDestinationIsRejected() throws Exception {
        final LogoutRequest logoutRequest = buildLogoutRequest();
        logoutRequest.setDestination("https://attacker.bot.net/");

        final String query = buildSignedQueryString(logoutRequest);

        final SamlLogoutRequestHandler handler = buildHandler();
        final ElasticsearchSecurityException exception = expectSamlException(() -> handler.parseFromQueryString(query));
        assertThat(exception.getMessage(), containsString("destination"));
        assertThat(exception.getMessage(), containsString(LOGOUT_URL));
        assertThat(exception.getMessage(), containsString(logoutRequest.getDestination()));
    }

    public void testLogoutWithSwitchedSignatureFailsValidation() throws Exception {
        final LogoutRequest fakeLogoutRequest = buildLogoutRequest();
        final LogoutRequest realLogoutRequest = buildLogoutRequest();
        final String fakeQuery = buildSignedQueryString(fakeLogoutRequest);
        final String realQuery = buildSignedQueryString(realLogoutRequest);

        final String tamperedQuery = fakeQuery.replaceFirst("&Signature=.*$", "")
            + "&Signature="
            + realQuery.replaceFirst("^.*&Signature=", "");

        final SamlLogoutRequestHandler handler = buildHandler();
        final ElasticsearchSecurityException exception = expectSamlException(() -> handler.parseFromQueryString(tamperedQuery));
        assertThat(exception.getMessage(), containsString("SAML Signature"));
        assertThat(exception.getMessage(), containsString("could not be validated"));
    }

    public void testLogoutWithSwitchedAlgorithmFailsValidation() throws Exception {
        final LogoutRequest logoutRequest = buildLogoutRequest();
        final String realQuery = buildSignedQueryString(logoutRequest);

        final String tamperedQuery = realQuery.replaceFirst(
            urlEncode(SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA256),
            urlEncode(SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA1)
        );

        final SamlLogoutRequestHandler handler = buildHandler();
        assertThat(handler.parseFromQueryString(realQuery), notNullValue());

        final ElasticsearchSecurityException exception = expectSamlException(() -> handler.parseFromQueryString(tamperedQuery));
        assertThat(exception.getMessage(), containsString("SAML Signature"));
        assertThat(exception.getMessage(), containsString("could not be validated"));
    }

    public void testLogoutWithoutSignatureFails() throws Exception {
        final LogoutRequest logoutRequest = buildLogoutRequest();
        final String query = buildQueryString(logoutRequest, new String[0]);
        final SamlLogoutRequestHandler handler = buildHandler();
        final ElasticsearchSecurityException exception = expectSamlException(() -> handler.parseFromQueryString(query));
        assertThat(exception.getMessage(), containsString("is not signed"));
    }

    /**
     * The spec states that this should never happen (SAML bindings spec, v2.0 WD6, section 3.4.4.1, point 3).
     * OneLogin includes newlines anyway.
     */
    public void testWhiteSpaceInSamlRequestIsIgnored() throws Exception {
        final LogoutRequest logoutRequest = buildLogoutRequest();
        final SigningConfiguration signingConfiguration = new SigningConfiguration(Sets.newHashSet("*"), credential);
        final String url = new SamlRedirect(logoutRequest, signingConfiguration) {
            @Override
            protected String deflateAndBase64Encode(SAMLObject message) throws Exception {
                return super.deflateAndBase64Encode(message).replaceAll(".{48}", "$0\n");
            }
        }.getRedirectUrl();
        final String query = new URI(url).getRawQuery();
        final SamlLogoutRequestHandler handler = buildHandler();
        final SamlLogoutRequestHandler.Result result = handler.parseFromQueryString(query);
        assertResultMatchesRequest(result, logoutRequest);
    }

    public void testRelayStateIsReturnedInRedirect() throws Exception {
        final LogoutRequest logoutRequest = buildLogoutRequest();
        final SigningConfiguration signingConfiguration = new SigningConfiguration(Sets.newHashSet("*"), credential);
        final String url = new SamlRedirect(logoutRequest, signingConfiguration).getRedirectUrl("Hail Hydra");
        final String query = new URI(url).getRawQuery();
        final SamlLogoutRequestHandler handler = buildHandler();
        final SamlLogoutRequestHandler.Result result = handler.parseFromQueryString(query);
        assertResultMatchesRequest(result, logoutRequest);
        assertThat(result.getRelayState(), equalTo("Hail Hydra"));
    }

    private String urlEncode(String str) {
        return URLEncoder.encode(str, StandardCharsets.US_ASCII);
    }

    private String buildSignedQueryString(LogoutRequest logoutRequest) throws URISyntaxException {
        return buildQueryString(logoutRequest, "*");
    }

    private String buildQueryString(LogoutRequest logoutRequest, String... signTypes) throws URISyntaxException {
        final SigningConfiguration signingConfiguration = new SigningConfiguration(Sets.newHashSet(signTypes), credential);
        final String url = new SamlRedirect(logoutRequest, signingConfiguration).getRedirectUrl();
        return new URI(url).getRawQuery();
    }

    private LogoutRequest buildLogoutRequest() {
        final LogoutRequest logoutRequest = SamlUtils.buildObject(LogoutRequest.class, LogoutRequest.DEFAULT_ELEMENT_NAME);
        logoutRequest.setDestination(LOGOUT_URL);
        logoutRequest.setIssueInstant(clock.instant());
        logoutRequest.setID(SamlUtils.generateSecureNCName(randomIntBetween(8, 30)));
        final Issuer issuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(IDP_ENTITY_ID);
        logoutRequest.setIssuer(issuer);
        final NameID nameId = SamlUtils.buildObject(NameID.class, NameID.DEFAULT_ELEMENT_NAME);
        nameId.setValue(randomAlphaOfLengthBetween(12, 36));
        logoutRequest.setNameID(nameId);
        return logoutRequest;
    }

    private SamlLogoutRequestHandler buildHandler() throws Exception {
        final IdpConfiguration idp = new IdpConfiguration(IDP_ENTITY_ID, () -> Collections.singletonList(credential));

        final X509Credential spCredential = (X509Credential) buildOpenSamlCredential(readRandomKeyPair()).get(0);
        final SigningConfiguration signingConfiguration = new SigningConfiguration(Collections.singleton("*"), spCredential);
        final SpConfiguration sp = new SingleSamlSpConfiguration(
            "https://sp.test/",
            "https://sp.test/saml/asc",
            LOGOUT_URL,
            signingConfiguration,
            Arrays.asList(spCredential),
            Collections.emptyList()
        );
        return new SamlLogoutRequestHandler(clock, idp, sp, TimeValue.timeValueSeconds(1));
    }

}
