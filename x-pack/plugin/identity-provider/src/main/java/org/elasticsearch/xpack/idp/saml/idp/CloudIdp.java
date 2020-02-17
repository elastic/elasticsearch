/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.idp;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.X509KeyPairSettings;
import org.elasticsearch.xpack.idp.saml.sp.CloudServiceProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.metadata.ContactPersonTypeEnumeration;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.security.x509.impl.X509KeyManagerX509CredentialAdapter;

import javax.net.ssl.X509KeyManager;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

public class CloudIdp implements SamlIdentityProvider {

    private final String entityId;
    private final HashMap<String, String> ssoEndpoints = new HashMap<>();
    private final HashMap<String, String> sloEndpoints = new HashMap<>();
    private final X509Credential signingCredential;
    private final X509Credential metadataSigningCredential;
    private Map<String, SamlServiceProvider> registeredServiceProviders;
    private SamlIdPMetadataBuilder.ContactInfo technicalContact;
    private SamlIdPMetadataBuilder.OrganizationInfo organization;

    public CloudIdp(Environment env, Settings settings) {
        this.entityId = require(settings, IDP_ENTITY_ID);
        this.ssoEndpoints.put(SAML2_REDIRECT_BINDING_URI, require(settings, IDP_SSO_REDIRECT_ENDPOINT));
        if (settings.hasValue(IDP_SSO_POST_ENDPOINT.getKey())) {
            this.ssoEndpoints.put(SAML2_POST_BINDING_URI, IDP_SSO_POST_ENDPOINT.get(settings));
        }
        if (settings.hasValue(IDP_SLO_POST_ENDPOINT.getKey())) {
            this.sloEndpoints.put(SAML2_POST_BINDING_URI, IDP_SLO_POST_ENDPOINT.get(settings));
        }
        if (settings.hasValue(IDP_SLO_REDIRECT_ENDPOINT.getKey())) {
            this.sloEndpoints.put(SAML2_REDIRECT_BINDING_URI, IDP_SLO_REDIRECT_ENDPOINT.get(settings));
        }
        this.signingCredential = buildSigningCredential(env, settings, "xpack.idp.signing.");
        this.metadataSigningCredential = buildSigningCredential(env, settings, "xpack.idp.metadata_signing.");
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
    public String getSingleSignOnEndpoint(String binding) {
        return ssoEndpoints.get(binding);
    }

    @Override
    public String getSingleLogoutEndpoint(String binding) {
        return sloEndpoints.get(binding);
    }

    @Override
    public X509Credential getSigningCredential() {
        return signingCredential;
    }

    @Override
    public X509Credential getMetadataSigningCredential() {
        return metadataSigningCredential;
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

    static X509Credential buildSigningCredential(Environment environment, Settings settings, String prefix) {
        List<X509Credential> credentials = buildCredentials(environment, settings, prefix, false);
        if (credentials.isEmpty()) {
            return null;
        }
        return credentials.get(0);
    }

    static List<X509Credential> buildCredentials(Environment env, Settings settings, String prefix, boolean allowMultiple) {
        final X509KeyPairSettings keyPairSettings = X509KeyPairSettings.withPrefix(prefix, false);
        final X509KeyManager keyManager = CertParsingUtils.getKeyManager(keyPairSettings, settings, null, env);
        if (keyManager == null) {
            return Collections.emptyList();
        }

        final List<X509Credential> credentials = new ArrayList<>();
        final Set<String> selectedAliases = new HashSet<>();
        final String configAlias = settings.get(prefix + "keystore.alias");
        if (Strings.isNullOrEmpty(configAlias)) {
            final String[] rsaAliases = keyManager.getServerAliases("RSA", null);
            if (null != rsaAliases) {
                selectedAliases.addAll(Arrays.asList(rsaAliases));
            }
            final String[] ecAliases = keyManager.getServerAliases("EC", null);
            if (null != ecAliases) {
                selectedAliases.addAll(Arrays.asList(ecAliases));
            }
            if (selectedAliases.isEmpty()) {
                throw new IllegalArgumentException(
                    "The configured keystore for [" + prefix + "keystore] does not contain any RSA or EC key pairs.");
            }
            if (selectedAliases.size() > 1 && allowMultiple == false) {
                throw new IllegalArgumentException(
                    "The configured keystore for [" + prefix + "keystore] contains multiple private key entries, when one was expected.");
            }
        } else {
            selectedAliases.add(configAlias);
        }
        for (String alias : selectedAliases) {
            final PrivateKey signingKey = keyManager.getPrivateKey(alias);
            if (signingKey == null) {
                throw new IllegalArgumentException("The configured keystore for [" + prefix + "keystore] does not have a private key" +
                    " associated with alias [" + alias + "]");
            }

            final String keyType = signingKey.getAlgorithm();
            if (keyType.equals("RSA") == false && keyType.equals("EC") == false) {
                throw new IllegalArgumentException("The key associated with alias [" + alias + "] " + "that has been configured with ["
                    + prefix + "keystore.alias] uses unsupported key algorithm type [" + keyType + "], only RSA and EC are supported");
            }
            credentials.add(new X509KeyManagerX509CredentialAdapter(keyManager, alias));
        }
        return credentials;
    }

    private Map<String, SamlServiceProvider> gatherRegisteredServiceProviders() {
        // TODO Fetch all the registered service providers from the index (?) they are persisted.
        // For now hardcode something to use.
        Map<String, SamlServiceProvider> registeredSps = new HashMap<>();
        registeredSps.put("kibana_url", new CloudServiceProvider("kibana_url", "kibana_url/api/security/v1/saml",
            NameID.TRANSIENT, false, false,null));

        return registeredSps;
    }


}
