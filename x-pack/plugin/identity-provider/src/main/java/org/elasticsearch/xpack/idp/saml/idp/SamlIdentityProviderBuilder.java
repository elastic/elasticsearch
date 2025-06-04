/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.idp;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.env.Environment;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderResolver;
import org.elasticsearch.xpack.idp.saml.sp.ServiceProviderDefaults;
import org.elasticsearch.xpack.idp.saml.sp.WildcardServiceProviderResolver;
import org.opensaml.saml.saml2.metadata.ContactPersonTypeEnumeration;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.security.x509.impl.X509KeyManagerX509CredentialAdapter;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.X509KeyManager;

import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_POST_BINDING_URI;
import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_REDIRECT_BINDING_URI;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

/**
 * Builds a {@link SamlIdentityProvider} instance, from either direct properties, or defined node {@link Settings}.
 */
public class SamlIdentityProviderBuilder {

    private static final List<String> ALLOWED_NAMEID_FORMATS = List.of(TRANSIENT);
    public static final Setting<String> IDP_ENTITY_ID = Setting.simpleString("xpack.idp.entity_id", Setting.Property.NodeScope);

    public static final Setting<URL> IDP_SSO_REDIRECT_ENDPOINT = new Setting<>(
        "xpack.idp.sso_endpoint.redirect",
        "https:",
        value -> parseUrl("xpack.idp.sso_endpoint.redirect", value),
        Setting.Property.NodeScope
    );
    public static final Setting<URL> IDP_SSO_POST_ENDPOINT = new Setting<>(
        "xpack.idp.sso_endpoint.post",
        "https:",
        value -> parseUrl("xpack.idp.sso_endpoint.post", value),
        Setting.Property.NodeScope
    );
    public static final Setting<URL> IDP_SLO_REDIRECT_ENDPOINT = new Setting<>(
        "xpack.idp.slo_endpoint.redirect",
        "https:",
        value -> parseUrl("xpack.idp.slo_endpoint.redirect", value),
        Setting.Property.NodeScope
    );
    public static final Setting<URL> IDP_SLO_POST_ENDPOINT = new Setting<>(
        "xpack.idp.slo_endpoint.post",
        "https:",
        value -> parseUrl("xpack.idp.slo_endpoint.post", value),
        Setting.Property.NodeScope
    );
    public static final Setting<List<String>> IDP_ALLOWED_NAMEID_FORMATS = Setting.stringListSetting(
        "xpack.idp.allowed_nameid_formats",
        List.of(TRANSIENT),
        SamlIdentityProviderBuilder::validateNameIDs,
        Setting.Property.NodeScope
    );

    public static final Setting<String> IDP_SIGNING_KEY_ALIAS = Setting.simpleString(
        "xpack.idp.signing.keystore.alias",
        Setting.Property.NodeScope
    );
    public static final Setting<String> IDP_METADATA_SIGNING_KEY_ALIAS = Setting.simpleString(
        "xpack.idp.metadata.signing.keystore.alias",
        Setting.Property.NodeScope
    );

    public static final Setting<String> IDP_ORGANIZATION_NAME = Setting.simpleString(
        "xpack.idp.organization.name",
        Setting.Property.NodeScope
    );
    public static final Setting<String> IDP_ORGANIZATION_DISPLAY_NAME = Setting.simpleString(
        "xpack.idp.organization.display_name",
        IDP_ORGANIZATION_NAME,
        Setting.Property.NodeScope
    );
    public static final Setting<URL> IDP_ORGANIZATION_URL = new Setting<>(
        "xpack.idp.organization.url",
        "http:",
        value -> parseUrl("xpack.idp.organization.url", value),
        Setting.Property.NodeScope
    );

    public static final Setting<String> IDP_CONTACT_GIVEN_NAME = Setting.simpleString(
        "xpack.idp.contact.given_name",
        Setting.Property.NodeScope
    );
    public static final Setting<String> IDP_CONTACT_SURNAME = Setting.simpleString("xpack.idp.contact.surname", Setting.Property.NodeScope);
    public static final Setting<String> IDP_CONTACT_EMAIL = Setting.simpleString("xpack.idp.contact.email", Setting.Property.NodeScope);

    private final SamlServiceProviderResolver serviceProviderResolver;
    private final WildcardServiceProviderResolver wildcardServiceResolver;

    private String entityId;
    private Map<String, URL> ssoEndpoints;
    private Map<String, URL> sloEndpoints;
    private Set<String> allowedNameIdFormats;
    private X509Credential signingCredential;
    private X509Credential metadataSigningCredential;
    private SamlIdentityProvider.ContactInfo technicalContact;
    private SamlIdentityProvider.OrganizationInfo organization;
    private ServiceProviderDefaults serviceProviderDefaults;

    SamlIdentityProviderBuilder(SamlServiceProviderResolver serviceProviderResolver, WildcardServiceProviderResolver wildcardResolver) {
        this.serviceProviderResolver = serviceProviderResolver;
        this.wildcardServiceResolver = wildcardResolver;
        this.ssoEndpoints = new HashMap<>();
        this.sloEndpoints = new HashMap<>();
    }

    public SamlIdentityProvider build() throws ValidationException {
        ValidationException ex = new ValidationException();

        if (Strings.isNullOrEmpty(entityId)) {
            ex.addValidationError("IDP Entity ID must be set (was [" + entityId + "])");
        }

        if (ssoEndpoints == null || ssoEndpoints.containsKey(SAML2_REDIRECT_BINDING_URI) == false) {
            ex.addValidationError("The redirect ([ " + SAML2_REDIRECT_BINDING_URI + "]) SSO binding is required");
        }

        if (signingCredential == null) {
            ex.addValidationError("Signing credential must be specified");
        } else {
            try {
                validateSigningKey(signingCredential.getPrivateKey());
            } catch (ElasticsearchSecurityException e) {
                ex.addValidationError("Signing credential is invalid - " + e.getMessage());
            }
        }

        if (metadataSigningCredential != null) {
            try {
                validateSigningKey(metadataSigningCredential.getPrivateKey());
            } catch (ElasticsearchSecurityException e) {
                ex.addValidationError("Metadata signing credential is invalid - " + e.getMessage());
            }
        }

        if (serviceProviderDefaults == null) {
            ex.addValidationError("Service provider defaults must be specified");
        }

        if (allowedNameIdFormats == null || allowedNameIdFormats.isEmpty()) {
            ex.addValidationError("At least 1 allowed NameID format must be specified");
        }

        if (ex.validationErrors().isEmpty() == false) {
            throw ex;
        }

        return new SamlIdentityProvider(
            entityId,
            Map.copyOf(ssoEndpoints),
            sloEndpoints == null ? Map.of() : Map.copyOf(sloEndpoints),
            Set.copyOf(allowedNameIdFormats),
            signingCredential,
            metadataSigningCredential,
            technicalContact,
            organization,
            serviceProviderDefaults,
            serviceProviderResolver,
            wildcardServiceResolver
        );
    }

    public SamlIdentityProviderBuilder fromSettings(Environment env) {
        final Settings settings = env.settings();
        this.entityId = require(settings, IDP_ENTITY_ID);
        this.ssoEndpoints = new HashMap<>();
        this.sloEndpoints = new HashMap<>();
        this.ssoEndpoints.put(SAML2_REDIRECT_BINDING_URI, requiredUrl(settings, IDP_SSO_REDIRECT_ENDPOINT));
        if (IDP_SSO_POST_ENDPOINT.exists(settings)) {
            this.ssoEndpoints.put(SAML2_POST_BINDING_URI, IDP_SSO_POST_ENDPOINT.get(settings));
        }
        if (IDP_SLO_POST_ENDPOINT.exists(settings)) {
            this.sloEndpoints.put(SAML2_POST_BINDING_URI, IDP_SLO_POST_ENDPOINT.get(settings));
        }
        if (IDP_SLO_REDIRECT_ENDPOINT.exists(settings)) {
            this.sloEndpoints.put(SAML2_REDIRECT_BINDING_URI, IDP_SLO_REDIRECT_ENDPOINT.get(settings));
        }
        this.allowedNameIdFormats = new HashSet<>(IDP_ALLOWED_NAMEID_FORMATS.get(settings));
        this.signingCredential = buildSigningCredential(env, settings, "xpack.idp.signing.");
        this.metadataSigningCredential = buildSigningCredential(env, settings, "xpack.idp.metadata_signing.");
        this.technicalContact = buildContactInfo(settings);
        this.organization = buildOrganization(settings);
        return this;
    }

    public static List<? extends Setting<?>> getSettings() {
        return List.of(
            IDP_ENTITY_ID,
            IDP_SLO_REDIRECT_ENDPOINT,
            IDP_SLO_POST_ENDPOINT,
            IDP_SSO_REDIRECT_ENDPOINT,
            IDP_SSO_POST_ENDPOINT,
            IDP_ALLOWED_NAMEID_FORMATS,
            IDP_SIGNING_KEY_ALIAS,
            IDP_METADATA_SIGNING_KEY_ALIAS,
            IDP_ORGANIZATION_NAME,
            IDP_ORGANIZATION_DISPLAY_NAME,
            IDP_ORGANIZATION_URL,
            IDP_CONTACT_GIVEN_NAME,
            IDP_CONTACT_SURNAME,
            IDP_CONTACT_EMAIL
        );
    }

    public SamlIdentityProviderBuilder serviceProviderDefaults(ServiceProviderDefaults serviceProviderDefaults) {
        this.serviceProviderDefaults = serviceProviderDefaults;
        return this;
    }

    public SamlIdentityProviderBuilder entityId(String entityId) {
        this.entityId = entityId;
        return this;
    }

    public SamlIdentityProviderBuilder singleSignOnEndpoints(Map<String, URL> ssoEndpointsMap) {
        this.ssoEndpoints = ssoEndpointsMap;
        return this;
    }

    public SamlIdentityProviderBuilder singleLogoutEndpoints(Map<String, URL> sloEndpointsMap) {
        this.sloEndpoints = sloEndpointsMap;
        return this;
    }

    public SamlIdentityProviderBuilder singleSignOnEndpoint(String binding, URL endpoint) {
        this.ssoEndpoints.put(binding, endpoint);
        return this;
    }

    public SamlIdentityProviderBuilder singleLogoutEndpoint(String binding, URL endpoint) {
        this.sloEndpoints.put(binding, endpoint);
        return this;
    }

    public SamlIdentityProviderBuilder allowedNameIdFormat(String nameIdFormat) {
        if (this.allowedNameIdFormats == null) {
            this.allowedNameIdFormats = new HashSet<>();
        }
        this.allowedNameIdFormats.add(nameIdFormat);
        return this;
    }

    public SamlIdentityProviderBuilder signingCredential(X509Credential signingCredential) {
        this.signingCredential = signingCredential;
        return this;
    }

    public SamlIdentityProviderBuilder metadataSigningCredential(X509Credential metadataSigningCredential) {
        this.metadataSigningCredential = metadataSigningCredential;
        return this;
    }

    public SamlIdentityProviderBuilder technicalContact(SamlIdentityProvider.ContactInfo technicalContact) {
        this.technicalContact = technicalContact;
        return this;
    }

    public SamlIdentityProviderBuilder organization(SamlIdentityProvider.OrganizationInfo organization) {
        this.organization = organization;
        return this;
    }

    private static URL parseUrl(String key, String value) {
        try {
            return new URL(value);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid value [" + value + "] for [" + key + "]. Not a valid URL", e);
        }
    }

    private static void validateNameIDs(List<String> values) {
        final Set<String> invalidFormats = values.stream()
            .distinct()
            .filter(e -> ALLOWED_NAMEID_FORMATS.contains(e) == false)
            .collect(Collectors.toSet());
        if (invalidFormats.size() > 0) {
            throw new IllegalArgumentException(
                invalidFormats + " are not valid NameID formats. Allowed values are " + ALLOWED_NAMEID_FORMATS
            );
        }
    }

    static String require(Settings settings, Setting<String> setting) {
        if (settings.hasValue(setting.getKey())) {
            return setting.get(settings);
        } else {
            throw new IllegalArgumentException("The configuration setting [" + setting.getKey() + "] is required");
        }
    }

    static URL requiredUrl(Settings settings, Setting<URL> setting) {
        if (settings.hasValue(setting.getKey())) {
            return setting.get(settings);
        } else {
            throw new IllegalArgumentException("The configuration setting [" + setting.getKey() + "] is required");
        }
    }

    // Package protected for testing
    static X509Credential buildSigningCredential(Environment environment, Settings settings, String prefix) {
        List<X509Credential> credentials = buildCredentials(environment, settings, prefix, false);
        if (credentials.isEmpty()) {
            return null;
        }
        return credentials.get(0);
    }

    static List<X509Credential> buildCredentials(Environment env, Settings settings, String prefix, boolean allowMultiple) {
        final SslKeyConfig keyConfig = CertParsingUtils.createKeyConfig(settings, prefix, env, false);
        if (keyConfig.hasKeyMaterial() == false) {
            return List.of();
        }
        final X509KeyManager keyManager = keyConfig.createKeyManager();
        if (keyManager == null) {
            return List.of();
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
                    "The configured keystore for [" + prefix + "keystore] does not contain any RSA or EC key pairs."
                );
            }
            if (selectedAliases.size() > 1 && allowMultiple == false) {
                throw new IllegalArgumentException(
                    "The configured keystore for [" + prefix + "keystore] contains multiple private key entries, when one was expected."
                );
            }
        } else {
            selectedAliases.add(configAlias);
        }
        for (String alias : selectedAliases) {
            try {
                validateSigningKey(keyManager.getPrivateKey(alias));
            } catch (ElasticsearchSecurityException e) {
                throw new IllegalArgumentException(
                    "The configured credential ["
                        + prefix
                        + "keystore] with alias ["
                        + alias
                        + "] is not a valid signing key - "
                        + e.getMessage()
                );
            }
            credentials.add(new X509KeyManagerX509CredentialAdapter(keyManager, alias));
        }
        return credentials;
    }

    private static void validateSigningKey(PrivateKey privateKey) {
        if (privateKey == null) {
            throw new ElasticsearchSecurityException("There is no private key available for this credential");
        }
        final String keyType = privateKey.getAlgorithm();
        if (keyType.equals("RSA") == false && keyType.equals("EC") == false) {
            throw new ElasticsearchSecurityException(
                "The private key uses unsupported key algorithm type [" + keyType + "], only RSA and EC are supported"
            );
        }
    }

    private static SamlIdentityProvider.OrganizationInfo buildOrganization(Settings settings) {
        final String name = settings.hasValue(IDP_ORGANIZATION_NAME.getKey()) ? IDP_ORGANIZATION_NAME.get(settings) : null;
        final String displayName = settings.hasValue(IDP_ORGANIZATION_DISPLAY_NAME.getKey())
            ? IDP_ORGANIZATION_DISPLAY_NAME.get(settings)
            : null;
        final String url = settings.hasValue(IDP_ORGANIZATION_URL.getKey()) ? IDP_ORGANIZATION_URL.get(settings).toString() : null;
        if (Stream.of(name, displayName, url).allMatch(Objects::isNull) == false) {
            return new SamlIdentityProvider.OrganizationInfo(name, displayName, url);
        }
        return null;
    }

    private static SamlIdentityProvider.ContactInfo buildContactInfo(Settings settings) {
        if (settings.hasValue(IDP_CONTACT_EMAIL.getKey())) {
            return new SamlIdentityProvider.ContactInfo(
                ContactPersonTypeEnumeration.TECHNICAL,
                IDP_CONTACT_GIVEN_NAME.get(settings),
                IDP_CONTACT_SURNAME.get(settings),
                IDP_CONTACT_EMAIL.get(settings)
            );
        }
        return null;
    }
}
