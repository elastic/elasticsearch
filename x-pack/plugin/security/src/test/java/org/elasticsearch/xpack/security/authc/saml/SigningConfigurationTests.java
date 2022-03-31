/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.common.util.set.Sets;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensaml.saml.common.SAMLObject;
import org.opensaml.saml.saml2.core.AuthnRequest;
import org.opensaml.saml.saml2.core.LogoutRequest;
import org.opensaml.security.x509.X509Credential;

import java.util.Arrays;

public class SigningConfigurationTests extends SamlTestCase {

    private static X509Credential credential;

    @BeforeClass
    public static void setupCredential() throws Exception {
        credential = (X509Credential) buildOpenSamlCredential(readRandomKeyPair()).get(0);
    }

    @AfterClass
    public static void clearCredential() throws Exception {
        credential = null;
    }

    public void testShouldSignObject() throws Exception {
        final AuthnRequest authnRequest = SamlUtils.buildObject(AuthnRequest.class, AuthnRequest.DEFAULT_ELEMENT_NAME);
        final LogoutRequest logoutRequest = SamlUtils.buildObject(LogoutRequest.class, LogoutRequest.DEFAULT_ELEMENT_NAME);

        assertShouldSign(authnRequest, "AuthnRequest");
        assertShouldSign(logoutRequest, "LogoutRequest");
        assertShouldSign(authnRequest, "*");
        assertShouldSign(logoutRequest, "*");
        assertShouldSign(authnRequest, "AuthnRequest", "LogoutRequest");
        assertShouldSign(logoutRequest, "AuthnRequest", "LogoutRequest");

        assertShouldNotSign(authnRequest, "LogoutRequest");
        assertShouldNotSign(logoutRequest, "AuthnRequest");
        assertShouldNotSign(authnRequest, new String[0]);
        assertShouldNotSign(logoutRequest, new String[0]);
        assertShouldNotSign(authnRequest, "foo", "bar", "baz");
        assertShouldNotSign(logoutRequest, "foo", "bar", "baz");
    }

    private void assertShouldSign(SAMLObject object, String... types) {
        final SigningConfiguration signingConfiguration = getSigningConfiguration(types);
        assertTrue("Configuration types " + Arrays.toString(types) + " should sign " + object, signingConfiguration.shouldSign(object));
    }

    private void assertShouldNotSign(SAMLObject object, String... types) {
        final SigningConfiguration signingConfiguration = getSigningConfiguration(types);
        assertFalse("Configuration types " + Arrays.toString(types) + " shouldn't sign " + object, signingConfiguration.shouldSign(object));
    }

    private SigningConfiguration getSigningConfiguration(String[] types) {
        return new SigningConfiguration(Sets.newHashSet(types), credential);
    }

}
