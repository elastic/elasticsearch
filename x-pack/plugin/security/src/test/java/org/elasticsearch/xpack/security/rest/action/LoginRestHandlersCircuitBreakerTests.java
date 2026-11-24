/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.rest.action.apikey.RestGrantApiKeyAction;
import org.elasticsearch.xpack.security.rest.action.oauth2.RestGetTokenAction;
import org.elasticsearch.xpack.security.rest.action.oauth2.RestInvalidateTokenAction;
import org.elasticsearch.xpack.security.rest.action.oidc.RestOpenIdConnectAuthenticateAction;
import org.elasticsearch.xpack.security.rest.action.oidc.RestOpenIdConnectLogoutAction;
import org.elasticsearch.xpack.security.rest.action.oidc.RestOpenIdConnectPrepareAuthenticationAction;
import org.elasticsearch.xpack.security.rest.action.profile.RestActivateProfileAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlAuthenticateAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlCompleteLogoutAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlInvalidateSessionAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlLogoutAction;
import org.elasticsearch.xpack.security.rest.action.saml.RestSamlPrepareAuthenticationAction;

import java.util.List;

import static org.hamcrest.Matchers.is;

/**
 * The calls that log users in and out or exchange credentials must keep working when the node is short on memory,
 * so the circuit breakers never reject them.
 */
public class LoginRestHandlersCircuitBreakerTests extends ESTestCase {

    public void testCanNotTripCircuitBreaker() {
        final Settings settings = Settings.EMPTY;
        final XPackLicenseState licenseState = new XPackLicenseState(() -> 0);
        final List<RestHandler> handlers = List.of(
            new RestSamlAuthenticateAction(settings, licenseState),
            new RestSamlPrepareAuthenticationAction(settings, licenseState),
            new RestSamlLogoutAction(settings, licenseState),
            new RestSamlInvalidateSessionAction(settings, licenseState),
            new RestSamlCompleteLogoutAction(settings, licenseState),
            new RestOpenIdConnectAuthenticateAction(settings, licenseState),
            new RestOpenIdConnectPrepareAuthenticationAction(settings, licenseState),
            new RestOpenIdConnectLogoutAction(settings, licenseState),
            new RestGetTokenAction(settings, licenseState),
            new RestInvalidateTokenAction(settings, licenseState),
            new RestGrantApiKeyAction(settings, licenseState, new RestGrantApiKeyAction.RequestTranslator.Default()),
            new RestDelegatePkiAuthenticationAction(settings, licenseState),
            new RestActivateProfileAction(settings, licenseState)
        );
        for (RestHandler handler : handlers) {
            assertThat(handler.getClass().getSimpleName(), handler.canTripCircuitBreaker(), is(false));
        }
    }
}
