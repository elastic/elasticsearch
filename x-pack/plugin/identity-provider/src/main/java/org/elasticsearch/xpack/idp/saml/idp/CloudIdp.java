/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.idp;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.idp.saml.sp.CloudServiceProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.opensaml.saml.saml2.metadata.ContactPersonTypeEnumeration;

import java.net.URL;
import java.util.HashMap;

import java.util.Map;
import java.util.Set;

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
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_POST_BINDING_URI;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_REDIRECT_BINDING_URI;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

public class CloudIdp implements SamlIdentityProvider {

    private final String entityId;
    private final HashMap<String, URL> ssoEndpoints = new HashMap<>();
    private final HashMap<String, URL> sloEndpoints = new HashMap<>();
    private Map<String, SamlServiceProvider> registeredServiceProviders;
    private SamlIdPMetadataBuilder.ContactInfo technicalContact;
    private SamlIdPMetadataBuilder.OrganizationInfo organization;

    public CloudIdp(Environment env, Settings settings) {
        this.entityId = require(settings, IDP_ENTITY_ID);
        this.ssoEndpoints.put(SAML2_REDIRECT_BINDING_URI, IDP_SSO_REDIRECT_ENDPOINT.get(settings));
        if (settings.hasValue(IDP_SSO_POST_ENDPOINT.getKey())) {
            this.ssoEndpoints.put(SAML2_POST_BINDING_URI, IDP_SSO_POST_ENDPOINT.get(settings));
        }
        if (settings.hasValue(IDP_SLO_POST_ENDPOINT.getKey())) {
            this.sloEndpoints.put(SAML2_POST_BINDING_URI, IDP_SLO_POST_ENDPOINT.get(settings));
        }
        if (settings.hasValue(IDP_SLO_REDIRECT_ENDPOINT.getKey())) {
            this.sloEndpoints.put(SAML2_REDIRECT_BINDING_URI, IDP_SLO_REDIRECT_ENDPOINT.get(settings));
        }
        this.registeredServiceProviders = gatherRegisteredServiceProviders();
        this.technicalContact = new SamlIdPMetadataBuilder.ContactInfo(ContactPersonTypeEnumeration.TECHNICAL,
            IDP_CONTACT_GIVEN_NAME.get(settings), IDP_CONTACT_SURNAME.get(settings), IDP_CONTACT_EMAIL.get(settings));
        this.organization = new SamlIdPMetadataBuilder.OrganizationInfo(IDP_ORGANIZATION_NAME.get(settings),
            IDP_ORGANIZATION_DISPLAY_NAME.get(settings), IDP_ORGANIZATION_URL.get(settings));
    }

    @Override
    public String getEntityId() {
        return entityId;
    }

    @Override
    public URL getSingleSignOnEndpoint(String binding) {
        return ssoEndpoints.get(binding);
    }

    @Override
    public URL getSingleLogoutEndpoint(String binding) {
        return sloEndpoints.get(binding);
    }

    @Override
    public SamlServiceProvider getRegisteredServiceProvider(String spEntityId) {
        return registeredServiceProviders.get(spEntityId);
    }

    @Override
    public SamlIdPMetadataBuilder.OrganizationInfo getOrganization() {
        return organization;
    }

    @Override
    public SamlIdPMetadataBuilder.ContactInfo getTechnicalContact() {
        return technicalContact;
    }

    private static String require(Settings settings, Setting<String> setting) {
        if (settings.hasValue(setting.getKey())) {
            return setting.get(settings);
        } else {
            throw new SettingsException("The configuration setting [" + setting.getKey() + "] is required");
        }
    }

    private Map<String, SamlServiceProvider> gatherRegisteredServiceProviders() {
        // TODO Fetch all the registered service providers from the index (?) they are persisted.
        // For now hardcode something to use.
        Map<String, SamlServiceProvider> registeredSps = new HashMap<>();
        registeredSps.put("https://sp.some.org",
            new CloudServiceProvider("https://sp.some.org", "https://sp.some.org/api/security/v1/saml", Set.of(TRANSIENT), null, false,
                false, null, null, null));
        return registeredSps;
    }


}
