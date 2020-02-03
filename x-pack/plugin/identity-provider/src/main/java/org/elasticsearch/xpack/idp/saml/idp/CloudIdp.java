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
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.X509KeyPairSettings;
import org.elasticsearch.xpack.idp.saml.sp.CloudKibanaServiceProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.security.x509.impl.X509KeyManagerX509CredentialAdapter;

import javax.net.ssl.X509KeyManager;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_ENTITY_ID;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SIGNING_KEY_ALIAS;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SLO_POST_ENDPOINT;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SLO_REDIRECT_ENDPOINT;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SSO_POST_ENDPOINT;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SSO_REDIRECT_ENDPOINT;

public class CloudIdp implements SamlIdentityProvider {

    private final String entityId;
    private final HashMap<String, String> ssoEndpoints = new HashMap<>();
    private final HashMap<String, String> sloEndpoints = new HashMap<>();
    private final X509Credential signingCredential;
    private Map<String, SamlServiceProvider> registeredServiceProviders;

    public CloudIdp(Environment env, Settings settings) {
        this.entityId = require(settings, IDP_ENTITY_ID);
        this.ssoEndpoints.put("redirect", require(settings, IDP_SSO_REDIRECT_ENDPOINT));
        if (settings.hasValue(IDP_SSO_POST_ENDPOINT.getKey())) {
            this.ssoEndpoints.put("post", settings.get(IDP_SSO_POST_ENDPOINT.getKey()));
        }
        if (settings.hasValue(IDP_SLO_POST_ENDPOINT.getKey())) {
            this.sloEndpoints.put("post", settings.get(IDP_SLO_POST_ENDPOINT.getKey()));
        }
        if (settings.hasValue(IDP_SLO_REDIRECT_ENDPOINT.getKey())) {
            this.sloEndpoints.put("redirect", settings.get(IDP_SLO_REDIRECT_ENDPOINT.getKey()));
        }
        this.signingCredential = buildSigningCredential(env, settings);
        this.registeredServiceProviders = gatherRegisteredServiceProviders();
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
    public Map<String, SamlServiceProvider> getRegisteredServiceProviders() {
        return registeredServiceProviders;
    }


    private static String require(Settings settings, Setting<String> setting) {
        if (settings.hasValue(setting.getKey())) {
            return setting.get(settings);
        } else {
            throw new SettingsException("The configuration setting [" + setting.getKey() + "] is required");
        }
    }

    static X509Credential buildSigningCredential(Environment env, Settings settings) {
        final X509KeyPairSettings keyPairSettings = X509KeyPairSettings.withPrefix("xpack.idp.signing.", false);
        final X509KeyManager keyManager = CertParsingUtils.getKeyManager(keyPairSettings, settings, null, env);
        if (keyManager == null) {
            return null;
        }

        final String selectedAlias;
        final String configAlias = IDP_SIGNING_KEY_ALIAS.get(settings);
        if (Strings.isNullOrEmpty(configAlias)) {
            final Set<String> aliases = new HashSet<>();
            final String[] rsaAliases = keyManager.getServerAliases("RSA", null);
            if (null != rsaAliases) {
                aliases.addAll(Arrays.asList(rsaAliases));
            }
            final String[] ecAliases = keyManager.getServerAliases("EC", null);
            if (null != ecAliases) {
                aliases.addAll(Arrays.asList(ecAliases));
            }
            if (aliases.isEmpty()) {
                throw new IllegalArgumentException(
                    "The configured keystore for xpack.idp.signing.keystore does not contain any RSA or EC key pairs");
            }
            if (aliases.size() > 1) {
                throw new IllegalArgumentException("The configured keystore for xpack.idp.signing.keystore contains multiple key pairs" +
                    " but no alias has been configured with [" + IDP_SIGNING_KEY_ALIAS.getKey() + "]");
            }
            selectedAlias = Iterables.get(aliases, 0);
        } else {
            selectedAlias = configAlias;
        }
        final PrivateKey signingKey = keyManager.getPrivateKey(selectedAlias);
        if (signingKey == null) {
            throw new IllegalArgumentException("The configured keystore for xpack.idp.signing.keystore does not have a private key" +
                " associated with alias [" + selectedAlias + "]");
        }

        final String keyType = signingKey.getAlgorithm();
        if (keyType.equals("RSA") == false && keyType.equals("EC") == false) {
            throw new IllegalArgumentException("The key associated with alias [" + selectedAlias + "] " + "that has been configured with ["
                + IDP_SIGNING_KEY_ALIAS.getKey() + "] uses unsupported key algorithm type [" + keyType
                + "], only RSA and EC are supported");
        }
        return new X509KeyManagerX509CredentialAdapter(keyManager, selectedAlias);
    }


    private Map<String, SamlServiceProvider> gatherRegisteredServiceProviders() {
        // TODO Fetch all the registered service providers from the index (?) they are persisted.
        // For now hardcode something to use.
        Map<String, SamlServiceProvider> registeredSps = new HashMap<>();
        registeredSps.put("kibana_url", new CloudKibanaServiceProvider("kibana_url", "kibana_url/api/security/v1/saml"));

        return registeredSps;
    }


}
