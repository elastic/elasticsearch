/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.LogoutResponse;
import org.opensaml.saml.saml2.core.Status;
import org.opensaml.saml.saml2.core.StatusCode;
import org.opensaml.saml.saml2.core.impl.StatusBuilder;
import org.opensaml.saml.saml2.core.impl.StatusCodeBuilder;
import org.opensaml.security.x509.X509Credential;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class SamlLogoutResponseHandlerHttpRedirectTests extends SamlTestCase {

    private static final String IDP_ENTITY_ID = "https://idp.test/";
    private static final String LOGOUT_URL = "https://sp.test/saml/logout";

    private Clock clock;
    private SamlLogoutResponseHandler samlLogoutResponseHandler;

    private static X509Credential credential;

    @BeforeClass
    public static void setupCredential() throws Exception {
        credential = (X509Credential) buildOpenSamlCredential(readRandomKeyPair()).get(0);
    }

    @AfterClass
    public static void clearCredential() {
        credential = null;
    }

    @Before
    public void setupHandler() throws Exception {
        clock = Clock.systemUTC();
        final IdpConfiguration idp = new IdpConfiguration(IDP_ENTITY_ID, () -> Collections.singletonList(credential));
        final X509Credential spCredential = (X509Credential) buildOpenSamlCredential(readRandomKeyPair()).get(0);
        final SigningConfiguration signingConfiguration = new SigningConfiguration(Collections.singleton("*"), spCredential);
        final SpConfiguration sp = new SpConfiguration(
            "https://sp.test/",
            "https://sp.test/saml/asc",
            LOGOUT_URL,
            signingConfiguration,
            List.of(spCredential),
            Collections.emptyList());
        samlLogoutResponseHandler = new SamlLogoutResponseHandler(clock, idp, sp, TimeValue.timeValueSeconds(1));
    }

    public void testHandlerWorks() throws URISyntaxException {
        final String requestId = SamlUtils.generateSecureNCName(randomIntBetween(8, 30));
        final SigningConfiguration signingConfiguration = new SigningConfiguration(Sets.newHashSet("*"), credential);
        final LogoutResponse logoutResponse = SamlUtils.buildObject(LogoutResponse.class, LogoutResponse.DEFAULT_ELEMENT_NAME);
        logoutResponse.setDestination(LOGOUT_URL);
        logoutResponse.setIssueInstant(new DateTime(clock.millis()));
        logoutResponse.setID(SamlUtils.generateSecureNCName(randomIntBetween(8, 30)));
        logoutResponse.setInResponseTo(requestId);
        logoutResponse.setStatus(buildStatus(StatusCode.SUCCESS));

        final Issuer issuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(IDP_ENTITY_ID);
        logoutResponse.setIssuer(issuer);
        final String url = new SamlRedirect(logoutResponse, signingConfiguration).getRedirectUrl();
        samlLogoutResponseHandler.handle(true, new URI(url).getRawQuery(), List.of(requestId));
    }

    public void testHandlerFailsIfStatusIsNotSuccess() {
        final String requestId = SamlUtils.generateSecureNCName(randomIntBetween(8, 30));
        final SigningConfiguration signingConfiguration = new SigningConfiguration(Sets.newHashSet("*"), credential);
        final LogoutResponse logoutResponse = SamlUtils.buildObject(LogoutResponse.class, LogoutResponse.DEFAULT_ELEMENT_NAME);
        logoutResponse.setDestination(LOGOUT_URL);
        logoutResponse.setIssueInstant(new DateTime(clock.millis()));
        logoutResponse.setID(SamlUtils.generateSecureNCName(randomIntBetween(8, 30)));
        logoutResponse.setInResponseTo(requestId);
        logoutResponse.setStatus(buildStatus(randomFrom(StatusCode.REQUESTER, StatusCode.RESPONDER)));

        final Issuer issuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(IDP_ENTITY_ID);
        logoutResponse.setIssuer(issuer);
        final String url = new SamlRedirect(logoutResponse, signingConfiguration).getRedirectUrl();

        final ElasticsearchSecurityException e =
            expectSamlException(() -> samlLogoutResponseHandler.handle(true, new URI(url).getRawQuery(), List.of(requestId)));
        assertThat(e.getMessage(), containsString("is not a 'success' response"));
    }

    public void testHandlerWillFailWhenQueryStringNotSigned() {
        final String requestId = SamlUtils.generateSecureNCName(randomIntBetween(8, 30));
        final SigningConfiguration signingConfiguration = new SigningConfiguration(Sets.newHashSet("*"), null);
        final LogoutResponse logoutResponse = SamlUtils.buildObject(LogoutResponse.class, LogoutResponse.DEFAULT_ELEMENT_NAME);
        logoutResponse.setDestination(LOGOUT_URL);
        logoutResponse.setIssueInstant(new DateTime(clock.millis()));
        logoutResponse.setID(SamlUtils.generateSecureNCName(randomIntBetween(8, 30)));
        logoutResponse.setInResponseTo(requestId);
        logoutResponse.setStatus(buildStatus(randomFrom(StatusCode.REQUESTER, StatusCode.RESPONDER)));

        final Issuer issuer = SamlUtils.buildObject(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(IDP_ENTITY_ID);
        logoutResponse.setIssuer(issuer);
        final String url = new SamlRedirect(logoutResponse, signingConfiguration).getRedirectUrl();
        final ElasticsearchSecurityException e =
            expectSamlException(() -> samlLogoutResponseHandler.handle(true, new URI(url).getRawQuery(), List.of(requestId)));
        assertThat(e.getMessage(), containsString("Query string is not signed, but is required for HTTP-Redirect binding"));
    }

    private Status buildStatus(String statusCodeValue) {
        final Status status = new StatusBuilder().buildObject();
        final StatusCode statusCode = new StatusCodeBuilder().buildObject();
        statusCode.setValue(statusCodeValue);
        status.setStatusCode(statusCode);
        return status;
    }

}
