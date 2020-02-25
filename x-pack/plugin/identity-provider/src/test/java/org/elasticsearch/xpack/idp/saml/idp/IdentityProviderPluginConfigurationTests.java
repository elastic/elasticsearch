/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.idp;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.hamcrest.Matchers;

import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_CONTACT_EMAIL;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_CONTACT_GIVEN_NAME;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_CONTACT_SURNAME;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_ENTITY_ID;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_ORGANIZATION_DISPLAY_NAME;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_ORGANIZATION_NAME;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_ORGANIZATION_URL;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SLO_POST_ENDPOINT;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SLO_REDIRECT_ENDPOINT;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SSO_POST_ENDPOINT;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SSO_REDIRECT_ENDPOINT;
import static org.hamcrest.Matchers.equalTo;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_POST_BINDING_URI;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_REDIRECT_BINDING_URI;

public class IdentityProviderPluginConfigurationTests extends IdpSamlTestCase {

    public void testAllSettings() throws Exception{
        Settings settings = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect")
            .put(IDP_SSO_POST_ENDPOINT.getKey(), "https://idp.org/sso/post")
            .put(IDP_SLO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/slo/redirect")
            .put(IDP_SLO_POST_ENDPOINT.getKey(), "https://idp.org/slo/post")
            .put(IDP_ORGANIZATION_NAME.getKey(), "organization_name")
            .put(IDP_ORGANIZATION_DISPLAY_NAME.getKey(), "organization_display_name")
            .put(IDP_ORGANIZATION_URL.getKey(), "https://idp.org")
            .put(IDP_CONTACT_GIVEN_NAME.getKey(), "Tony")
            .put(IDP_CONTACT_SURNAME.getKey(), "Stark")
            .put(IDP_CONTACT_EMAIL.getKey(), "tony@starkindustries.com")
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        CloudIdp idp = new CloudIdp(env, settings);
        assertThat(idp.getEntityId(), equalTo("urn:elastic:cloud:idp"));
        assertThat(idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI).toString(), equalTo("https://idp.org/sso/redirect"));
        assertThat(idp.getSingleSignOnEndpoint(SAML2_POST_BINDING_URI).toString(), equalTo("https://idp.org/sso/post"));
        assertThat(idp.getSingleLogoutEndpoint(SAML2_REDIRECT_BINDING_URI).toString(), equalTo("https://idp.org/slo/redirect"));
        assertThat(idp.getSingleLogoutEndpoint(SAML2_POST_BINDING_URI).toString(), equalTo("https://idp.org/slo/post"));
        assertThat(idp.getOrganization(), equalTo(new SamlIdPMetadataBuilder.OrganizationInfo("organization_name",
            "organization_display_name", "https://idp.org")));
    }

    public void testInvalidSsoEndpoint() {
        Settings settings = Settings.builder()
            .put("path.home", LuceneTestCase.createTempDir())
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "not a url")
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        IllegalArgumentException e = LuceneTestCase.expectThrows(IllegalArgumentException.class, () -> new CloudIdp(env, settings));
        assertThat(e.getMessage(), Matchers.containsString(IDP_SSO_REDIRECT_ENDPOINT.getKey()));
        assertThat(e.getMessage(), Matchers.containsString("Not a valid URL"));
    }
}
