/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.idp;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.X509KeyPairSettings;
import org.elasticsearch.xpack.idp.saml.sp.CloudServiceProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.opensaml.saml.saml2.metadata.ContactPersonTypeEnumeration;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.security.x509.impl.X509KeyManagerX509CredentialAdapter;

import javax.net.ssl.X509KeyManager;
import java.net.URL;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

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
    private final X509Credential signingCredential;
    private final X509Credential metadataSigningCredential;
    private SamlIdPMetadataBuilder.ContactInfo technicalContact;
    private SamlIdPMetadataBuilder.OrganizationInfo organization;

    public CloudIdp(Environment env, Settings settings) {
        this.entityId = require(settings, IDP_ENTITY_ID);
        this.ssoEndpoints.put(SAML2_REDIRECT_BINDING_URI, requiredUrl(settings, IDP_SSO_REDIRECT_ENDPOINT));
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
        this.signingCredential = buildSigningCredential(env, settings, "xpack.idp.signing.");
        this.metadataSigningCredential = buildSigningCredential(env, settings, "xpack.idp.metadata_signing.");
        this.technicalContact = buildContactInfo(settings);
        this.organization = buildOrganization(settings);
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
    public X509Credential getSigningCredential() {
        return signingCredential;
    }

    @Override
    public X509Credential getMetadataSigningCredential() {
        return metadataSigningCredential;
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
            throw new IllegalArgumentException("The configuration setting [" + setting.getKey() + "] is required");
        }
    }

    private static URL requiredUrl(Settings settings, Setting<URL> setting) {
        if (settings.hasValue(setting.getKey())) {
            return setting.get(settings);
        } else {
            throw new IllegalArgumentException("The configuration setting [" + setting.getKey() + "] is required");
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

    private SamlIdPMetadataBuilder.OrganizationInfo buildOrganization(Settings settings) {
        final String name = settings.hasValue(IDP_ORGANIZATION_NAME.getKey()) ? IDP_ORGANIZATION_NAME.get(settings) : null;
        final String displayName = settings.hasValue(IDP_ORGANIZATION_DISPLAY_NAME.getKey()) ?
            IDP_ORGANIZATION_DISPLAY_NAME.get(settings) : null;
        final String url = settings.hasValue(IDP_ORGANIZATION_URL.getKey()) ? IDP_ORGANIZATION_URL.get(settings).toString() : null;
        if (Stream.of(name, displayName, url).allMatch(Objects::isNull) == false) {
            return new SamlIdPMetadataBuilder.OrganizationInfo(name, displayName, url);
        }
        return null;
    }

    private SamlIdPMetadataBuilder.ContactInfo buildContactInfo(Settings settings) {
        if (settings.hasValue(IDP_CONTACT_EMAIL.getKey())) {
            return new SamlIdPMetadataBuilder.ContactInfo(ContactPersonTypeEnumeration.TECHNICAL,
                IDP_CONTACT_GIVEN_NAME.get(settings), IDP_CONTACT_SURNAME.get(settings), IDP_CONTACT_EMAIL.get(settings));
        }
        return null;
    }

    private Map<String, SamlServiceProvider> gatherRegisteredServiceProviders() {
        // TODO Fetch all the registered service providers from the index (?) they are persisted.
        // For now hardcode something to use.
        Map<String, SamlServiceProvider> registeredSps = new HashMap<>();
        registeredSps.put("https://sp.some.org",
            new CloudServiceProvider("https://sp.some.org", "https://sp.some.org/api/security/v1/saml", Set.of(TRANSIENT), null, false,
                false, null));
        return registeredSps;
    }


}
