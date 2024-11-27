/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import net.shibboleth.utilities.java.support.component.ComponentInitializationException;
import net.shibboleth.utilities.java.support.resolver.CriteriaSet;
import net.shibboleth.utilities.java.support.resolver.ResolverException;
import net.shibboleth.utilities.java.support.xml.BasicParserPool;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.PrivilegedFileWatcher;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.support.DelegatedAuthorizationSupport;
import org.elasticsearch.xpack.security.authc.support.mapper.ExcludingRoleMapper;
import org.opensaml.core.criterion.EntityIdCriterion;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.criterion.EntityRoleCriterion;
import org.opensaml.saml.metadata.resolver.MetadataResolver;
import org.opensaml.saml.metadata.resolver.impl.AbstractReloadingMetadataResolver;
import org.opensaml.saml.metadata.resolver.impl.FilesystemMetadataResolver;
import org.opensaml.saml.metadata.resolver.impl.HTTPMetadataResolver;
import org.opensaml.saml.metadata.resolver.impl.PredicateRoleDescriptorResolver;
import org.opensaml.saml.saml2.core.AuthnRequest;
import org.opensaml.saml.saml2.core.LogoutRequest;
import org.opensaml.saml.saml2.core.LogoutResponse;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.StatusCode;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.IDPSSODescriptor;
import org.opensaml.saml.security.impl.MetadataCredentialResolver;
import org.opensaml.security.credential.Credential;
import org.opensaml.security.credential.UsageType;
import org.opensaml.security.criteria.UsageCriterion;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.security.x509.impl.X509KeyManagerX509CredentialAdapter;
import org.opensaml.xmlsec.keyinfo.impl.BasicProviderKeyInfoCredentialResolver;
import org.opensaml.xmlsec.keyinfo.impl.KeyInfoProvider;
import org.opensaml.xmlsec.keyinfo.impl.KeyInfoResolutionContext;
import org.opensaml.xmlsec.keyinfo.impl.provider.InlineX509DataProvider;

import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.GeneralSecurityException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.PublicKey;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.X509KeyManager;

import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.transport.Transports.assertNotTransportThread;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.CLOCK_SKEW;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.DN_ATTRIBUTE;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.ENCRYPTION_KEY_ALIAS;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.ENCRYPTION_SETTING_KEY;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.FORCE_AUTHN;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.GROUPS_ATTRIBUTE;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.IDP_ENTITY_ID;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.IDP_METADATA_HTTP_FAIL_ON_ERROR;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.IDP_METADATA_HTTP_MIN_REFRESH;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.IDP_METADATA_HTTP_REFRESH;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.IDP_METADATA_PATH;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.IDP_SINGLE_LOGOUT;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.MAIL_ATTRIBUTE;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.NAMEID_ALLOW_CREATE;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.NAMEID_FORMAT;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.NAMEID_SP_QUALIFIER;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.NAME_ATTRIBUTE;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.POPULATE_USER_METADATA;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.PRINCIPAL_ATTRIBUTE;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.REQUESTED_AUTHN_CONTEXT_CLASS_REF;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.SIGNING_KEY_ALIAS;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.SIGNING_MESSAGE_TYPES;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.SIGNING_SETTING_KEY;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.SP_ACS;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.SP_ENTITY_ID;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.SP_LOGOUT;

/**
 * This class is {@link Releasable} because it uses a library that thinks timers and timer tasks
 * are still cool and no chance to opt out
 */
public final class SamlRealm extends Realm implements Releasable {
    private static final Logger logger = LogManager.getLogger(SamlRealm.class);

    public static final String USER_METADATA_NAMEID_VALUE = "saml_" + SamlAttributes.NAMEID_SYNTHENTIC_ATTRIBUTE;
    public static final String USER_METADATA_NAMEID_FORMAT = USER_METADATA_NAMEID_VALUE + "_format";

    public static final String CONTEXT_TOKEN_DATA = "_xpack_saml_tokendata";
    public static final String TOKEN_METADATA_NAMEID_VALUE = "saml_nameid_val";
    public static final String TOKEN_METADATA_NAMEID_FORMAT = "saml_nameid_fmt";
    public static final String TOKEN_METADATA_NAMEID_QUALIFIER = "saml_nameid_qual";
    public static final String TOKEN_METADATA_NAMEID_SP_QUALIFIER = "saml_nameid_sp_qual";
    public static final String TOKEN_METADATA_NAMEID_SP_PROVIDED_ID = "saml_nameid_sp_id";
    public static final String TOKEN_METADATA_SESSION = "saml_session";
    public static final String TOKEN_METADATA_REALM = "saml_realm";
    // Although we only use this for IDP metadata loading, the SSLServer only loads configurations where "ssl." is a top-level element
    // in the realm group configuration, so it has to have this name.

    // Strictly, this shouldn't be static because it means that only 1 SAML realm per node can be refreshing metadata, but there's no reason
    // to assume that there's any relationship between SAML realms. However, because all the metadata loading code is in static methods, we
    // live with this limitation for now.
    private static final AtomicBoolean REFRESHING_METADATA = new AtomicBoolean(false);

    private final List<Releasable> releasables;

    private final SamlAuthenticator authenticator;
    private final SamlLogoutRequestHandler logoutHandler;
    private final UserRoleMapper roleMapper;

    private final SamlLogoutResponseHandler logoutResponseHandler;
    private final Supplier<EntityDescriptor> idpDescriptor;

    private final SpConfiguration serviceProvider;
    private final SamlAuthnRequestBuilder.NameIDPolicySettings nameIdPolicy;
    private final Boolean forceAuthn;
    private final boolean useSingleLogout;
    private final Boolean populateUserMetadata;

    private final AttributeParser principalAttribute;
    private final AttributeParser groupsAttribute;
    private final AttributeParser dnAttribute;
    private final AttributeParser nameAttribute;
    private final AttributeParser mailAttribute;

    private DelegatedAuthorizationSupport delegatedRealms;

    /**
     * Factory for SAML realm.
     * This is not a constructor as it needs to initialise a number of components before delegating to
     * {@link #SamlRealm}
     */
    public static SamlRealm create(
        RealmConfig config,
        SSLService sslService,
        ResourceWatcherService watcherService,
        UserRoleMapper roleMapper
    ) throws Exception {
        SamlUtils.initialize(logger);

        if (TokenService.isTokenServiceEnabled(config.settings()) == false) {
            throw new IllegalStateException(
                "SAML requires that the token service be enabled (" + XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey() + ")"
            );
        }

        final Tuple<AbstractReloadingMetadataResolver, Supplier<EntityDescriptor>> tuple = initializeResolver(
            logger,
            config,
            sslService,
            watcherService
        );
        final AbstractReloadingMetadataResolver metadataResolver = tuple.v1();
        final Supplier<EntityDescriptor> idpDescriptor = tuple.v2();

        final SpConfiguration serviceProvider = getSpConfiguration(config);

        final Clock clock = Clock.systemUTC();
        final IdpConfiguration idpConfiguration = getIdpConfiguration(config, metadataResolver, idpDescriptor);
        final TimeValue maxSkew = config.getSetting(CLOCK_SKEW);
        final SamlAuthenticator authenticator = new SamlAuthenticator(clock, idpConfiguration, serviceProvider, maxSkew);
        final SamlLogoutRequestHandler logoutHandler = new SamlLogoutRequestHandler(clock, idpConfiguration, serviceProvider, maxSkew);
        final SamlLogoutResponseHandler logoutResponseHandler = new SamlLogoutResponseHandler(
            clock,
            idpConfiguration,
            serviceProvider,
            maxSkew
        );

        final SamlRealm realm = new SamlRealm(
            config,
            roleMapper,
            authenticator,
            logoutHandler,
            logoutResponseHandler,
            idpDescriptor,
            serviceProvider
        );

        // the metadata resolver needs to be destroyed since it runs a timer task in the background and destroying stops it!
        realm.releasables.add(() -> metadataResolver.destroy());

        return realm;
    }

    public SpConfiguration getServiceProvider() {
        return serviceProvider;
    }

    // For testing
    SamlRealm(
        RealmConfig config,
        UserRoleMapper roleMapper,
        SamlAuthenticator authenticator,
        SamlLogoutRequestHandler logoutHandler,
        SamlLogoutResponseHandler logoutResponseHandler,
        Supplier<EntityDescriptor> idpDescriptor,
        SpConfiguration spConfiguration
    ) throws Exception {
        super(config);

        if (config.hasSetting(SamlRealmSettings.EXCLUDE_ROLES)) {
            this.roleMapper = new ExcludingRoleMapper(roleMapper, config.getSetting(SamlRealmSettings.EXCLUDE_ROLES));
        } else {
            this.roleMapper = roleMapper;
        }
        this.authenticator = authenticator;
        this.logoutHandler = logoutHandler;
        this.logoutResponseHandler = logoutResponseHandler;

        this.idpDescriptor = idpDescriptor;
        this.serviceProvider = spConfiguration;

        this.nameIdPolicy = new SamlAuthnRequestBuilder.NameIDPolicySettings(
            config.getSetting(NAMEID_FORMAT),
            config.getSetting(NAMEID_ALLOW_CREATE),
            config.getSetting(NAMEID_SP_QUALIFIER)
        );
        this.forceAuthn = config.getSetting(FORCE_AUTHN, () -> null);
        this.useSingleLogout = config.getSetting(IDP_SINGLE_LOGOUT);
        this.populateUserMetadata = config.getSetting(POPULATE_USER_METADATA);
        this.principalAttribute = AttributeParser.forSetting(logger, PRINCIPAL_ATTRIBUTE, config, true);

        this.groupsAttribute = AttributeParser.forSetting(logger, GROUPS_ATTRIBUTE, config);
        this.dnAttribute = AttributeParser.forSetting(logger, DN_ATTRIBUTE, config, false);
        this.nameAttribute = AttributeParser.forSetting(logger, NAME_ATTRIBUTE, config, false);
        this.mailAttribute = AttributeParser.forSetting(logger, MAIL_ATTRIBUTE, config, false);

        this.releasables = new ArrayList<>();
    }

    @Override
    public void initialize(Iterable<Realm> realms, XPackLicenseState licenseState) {
        if (delegatedRealms != null) {
            throw new IllegalStateException("Realm has already been initialized");
        }
        delegatedRealms = new DelegatedAuthorizationSupport(realms, config, licenseState);
    }

    static String require(RealmConfig config, Setting.AffixSetting<String> setting) {
        final String value = config.getSetting(setting);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(
                "The configuration setting [" + RealmSettings.getFullSettingKey(config, setting) + "] is required"
            );
        }
        return value;
    }

    static IdpConfiguration getIdpConfiguration(
        RealmConfig config,
        MetadataResolver metadataResolver,
        Supplier<EntityDescriptor> idpDescriptor
    ) {
        final MetadataCredentialResolver resolver = new MetadataCredentialResolver();

        final PredicateRoleDescriptorResolver roleDescriptorResolver = new PredicateRoleDescriptorResolver(metadataResolver);
        resolver.setRoleDescriptorResolver(roleDescriptorResolver);

        final List<KeyInfoProvider> keyInfoProviders = Collections.singletonList(new InlineX509DataProvider());
        final BasicProviderKeyInfoCredentialResolver credentialsResolver = new BasicProviderKeyInfoCredentialResolver(keyInfoProviders) {
            final AtomicReference<Set<PublicKey>> previousCredentialsRef = new AtomicReference<>();

            @Override
            protected void postProcess(KeyInfoResolutionContext kiContext, CriteriaSet criteriaSet, List<Credential> credentials)
                throws ResolverException {
                SamlRealm.logDiff(credentials, this.previousCredentialsRef);
                super.postProcess(kiContext, criteriaSet, credentials);
            }
        };
        resolver.setKeyInfoCredentialResolver(credentialsResolver);

        try {
            roleDescriptorResolver.initialize();
            resolver.initialize();
        } catch (ComponentInitializationException e) {
            throw new IllegalStateException("Cannot initialise SAML IDP resolvers for realm " + config.name(), e);
        }

        final String entityID = idpDescriptor.get().getEntityID();
        return new IdpConfiguration(entityID, () -> {
            try {
                final Iterable<Credential> credentials = resolver.resolve(
                    new CriteriaSet(
                        new EntityIdCriterion(entityID),
                        new EntityRoleCriterion(IDPSSODescriptor.DEFAULT_ELEMENT_NAME),
                        new UsageCriterion(UsageType.SIGNING)
                    )
                );
                return CollectionUtils.iterableAsArrayList(credentials);
            } catch (ResolverException e) {
                throw new IllegalStateException("Cannot resolve SAML IDP credentials resolver for realm " + config.name(), e);
            }
        });
    }

    private static void logDiff(final List<Credential> newCredentials, final AtomicReference<Set<PublicKey>> previousCredentialsRef) {
        if (newCredentials == null) {
            logger.warn("Signing credentials missing, null");
            return;
        } else if (newCredentials.isEmpty()) {
            logger.warn("Signing credentials missing, empty");
            return;
        }
        final Set<PublicKey> newPublicKeys = newCredentials.stream().map(Credential::getPublicKey).collect(Collectors.toSet());
        final Set<PublicKey> previousPublicKeys = previousCredentialsRef.getAndSet(newPublicKeys);
        if (previousPublicKeys == null) {
            logger.trace("Signing credentials initialized, added: [{}]", newCredentials.size());
        } else {
            final Set<PublicKey> added = Sets.difference(newPublicKeys, previousPublicKeys);
            final Set<PublicKey> removed = Sets.difference(previousPublicKeys, newPublicKeys);
            if (added.isEmpty() && removed.isEmpty()) {
                logger.debug("Signing credentials did not change, current: [{}]", newCredentials.size());
            } else {
                logger.info(
                    "Signing credentials changed, new: [{}], previous: [{}], added: [{}], removed: [{}]",
                    newCredentials.size(),
                    previousPublicKeys.size(),
                    added.size(),
                    removed.size()
                );
            }
        }
    }

    static SpConfiguration getSpConfiguration(RealmConfig config) throws IOException, GeneralSecurityException {
        final String serviceProviderId = require(config, SP_ENTITY_ID);
        final String assertionConsumerServiceURL = require(config, SP_ACS);
        final String logoutUrl = config.getSetting(SP_LOGOUT);
        final List<String> reqAuthnCtxClassRef = config.getSetting(REQUESTED_AUTHN_CONTEXT_CLASS_REF);
        return new SpConfiguration(
            serviceProviderId,
            assertionConsumerServiceURL,
            logoutUrl,
            buildSigningConfiguration(config),
            buildEncryptionCredential(config),
            reqAuthnCtxClassRef
        );
    }

    // Package-private for testing
    static List<X509Credential> buildEncryptionCredential(RealmConfig config) throws IOException, GeneralSecurityException {
        return buildCredential(
            config,
            RealmSettings.realmSettingPrefix(config.identifier()) + ENCRYPTION_SETTING_KEY,
            ENCRYPTION_KEY_ALIAS,
            true
        );
    }

    static SigningConfiguration buildSigningConfiguration(RealmConfig config) throws IOException, GeneralSecurityException {
        final List<X509Credential> credentials = buildCredential(
            config,
            RealmSettings.realmSettingPrefix(config.identifier()) + SIGNING_SETTING_KEY,
            SIGNING_KEY_ALIAS,
            false
        );
        if (credentials == null || credentials.isEmpty()) {
            if (config.hasSetting(SIGNING_MESSAGE_TYPES)) {
                throw new IllegalArgumentException(
                    "The setting ["
                        + RealmSettings.getFullSettingKey(config, SIGNING_MESSAGE_TYPES)
                        + "] cannot be specified if there are no signing credentials"
                );
            } else {
                return new SigningConfiguration(Collections.emptySet(), null);
            }
        } else {
            final List<String> types = config.getSetting(SIGNING_MESSAGE_TYPES);
            return new SigningConfiguration(Sets.newHashSet(types), credentials.get(0));
        }
    }

    private static List<X509Credential> buildCredential(
        RealmConfig config,
        String prefix,
        Setting.AffixSetting<String> aliasSetting,
        boolean allowMultiple
    ) {
        final SslKeyConfig keyConfig = CertParsingUtils.createKeyConfig(config.settings(), prefix, config.env(), false);
        if (keyConfig.hasKeyMaterial() == false) {
            return null;
        }
        final X509KeyManager keyManager = keyConfig.createKeyManager();
        if (keyManager == null) {
            return null;
        }

        final Set<String> aliases = new HashSet<>();
        final String configuredAlias = config.getSetting(aliasSetting);
        if (Strings.isNullOrEmpty(configuredAlias)) {

            final String[] serverAliases = keyManager.getServerAliases("RSA", null);
            if (serverAliases != null) {
                aliases.addAll(Arrays.asList(serverAliases));
            }

            if (aliases.isEmpty()) {
                throw new IllegalArgumentException("The configured key store for " + prefix + " does not contain any RSA key pairs");
            } else if (allowMultiple == false && aliases.size() > 1) {
                throw new IllegalArgumentException(
                    "The configured key store for "
                        + prefix
                        + " has multiple keys but no alias has been specified (from setting "
                        + RealmSettings.getFullSettingKey(config, aliasSetting)
                        + ")"
                );
            }
        } else {
            aliases.add(configuredAlias);
        }

        final List<X509Credential> credentials = new ArrayList<>();
        for (String alias : aliases) {
            if (keyManager.getPrivateKey(alias) == null) {
                throw new IllegalArgumentException(
                    "The configured key store for "
                        + prefix
                        + " does not have a key associated with alias ["
                        + alias
                        + "] "
                        + ((Strings.isNullOrEmpty(configuredAlias) == false)
                            ? "(from setting " + RealmSettings.getFullSettingKey(config, aliasSetting) + ")"
                            : "")
                );
            }

            final String keyType = keyManager.getPrivateKey(alias).getAlgorithm();
            if (keyType.equals("RSA") == false) {
                throw new IllegalArgumentException(
                    "The key associated with alias ["
                        + alias
                        + "] "
                        + "(from setting "
                        + RealmSettings.getFullSettingKey(config, aliasSetting)
                        + ") uses unsupported key algorithm type ["
                        + keyType
                        + "], only RSA is supported"
                );
            }
            credentials.add(new X509KeyManagerX509CredentialAdapter(keyManager, alias));
        }

        return credentials;
    }

    public static List<SamlRealm> findSamlRealms(Realms realms, String realmName, String acsUrl) {
        Stream<SamlRealm> stream = realms.stream().filter(r -> r instanceof SamlRealm).map(r -> (SamlRealm) r);
        if (Strings.hasText(realmName)) {
            stream = stream.filter(r -> realmName.equals(r.name()));
        }
        if (Strings.hasText(acsUrl)) {
            stream = stream.filter(r -> acsUrl.equals(r.assertionConsumerServiceURL()));
        }
        return stream.collect(Collectors.toList());
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof SamlToken;
    }

    private boolean isTokenForRealm(SamlToken samlToken) {
        if (samlToken.getAuthenticatingRealm() == null) {
            return true;
        } else {
            return samlToken.getAuthenticatingRealm().equals(this.name());
        }
    }

    /**
     * Always returns {@code null} as there is no support for reading a SAML token out of a request
     *
     * @see org.elasticsearch.xpack.security.action.saml.TransportSamlAuthenticateAction
     */
    @Override
    public AuthenticationToken token(ThreadContext threadContext) {
        return null;
    }

    @Override
    public void authenticate(AuthenticationToken authenticationToken, ActionListener<AuthenticationResult<User>> listener) {
        if (authenticationToken instanceof SamlToken && isTokenForRealm((SamlToken) authenticationToken)) {
            if (this.idpDescriptor.get() instanceof UnresolvedEntity) {
                // This isn't an ideal check, but we don't have a better option right now
                listener.onResponse(
                    AuthenticationResult.unsuccessful(
                        "SAML realm [" + this + "] cannot authenticate because the metadata could not be resolved",
                        null
                    )
                );
                return;
            }
            try {
                final SamlToken token = (SamlToken) authenticationToken;
                final SamlAttributes attributes = authenticator.authenticate(token);
                logger.debug("Parsed token [{}] to attributes [{}]", token, attributes);
                buildUser(attributes, listener);
            } catch (ElasticsearchSecurityException e) {
                if (SamlUtils.isSamlException(e)) {
                    listener.onResponse(AuthenticationResult.unsuccessful("Provided SAML response is not valid for realm " + this, e));
                } else {
                    listener.onFailure(e);
                }
            }
        } else {
            listener.onResponse(AuthenticationResult.notHandled());
        }
    }

    private void buildUser(SamlAttributes attributes, ActionListener<AuthenticationResult<User>> baseListener) {
        final String principal = resolveSingleValueAttribute(attributes, principalAttribute, PRINCIPAL_ATTRIBUTE.name(config));
        if (Strings.isNullOrEmpty(principal)) {
            final String msg = principalAttribute
                + " not found in saml attributes"
                + attributes.attributes()
                + " or NameID ["
                + attributes.name()
                + "]";
            baseListener.onResponse(AuthenticationResult.unsuccessful(msg, null));
            return;
        }

        final Map<String, Object> tokenMetadata = createTokenMetadata(attributes.name(), attributes.session());
        ActionListener<AuthenticationResult<User>> wrappedListener = baseListener.delegateFailureAndWrap((l, auth) -> {
            if (auth.isAuthenticated()) {
                // Add the SAML token details as metadata on the authentication
                Map<String, Object> metadata = new HashMap<>(auth.getMetadata());
                metadata.put(CONTEXT_TOKEN_DATA, tokenMetadata);
                auth = AuthenticationResult.success(auth.getValue(), metadata);
            }
            l.onResponse(auth);
        });

        if (delegatedRealms.hasDelegation()) {
            delegatedRealms.resolve(principal, wrappedListener);
            return;
        }

        final Map<String, Object> userMetaBuilder = new HashMap<>();
        if (populateUserMetadata) {
            for (SamlAttributes.SamlAttribute a : attributes.attributes()) {
                userMetaBuilder.put("saml(" + a.name + ")", a.values);
                if (Strings.hasText(a.friendlyName)) {
                    userMetaBuilder.put("saml_" + a.friendlyName, a.values);
                }
            }
        }
        if (attributes.name() != null) {
            userMetaBuilder.put(USER_METADATA_NAMEID_VALUE, attributes.name().value);
            if (attributes.name().format != null) {
                userMetaBuilder.put(USER_METADATA_NAMEID_FORMAT, attributes.name().format);
            }
        }
        final Map<String, Object> userMeta = Map.copyOf(userMetaBuilder);

        final List<String> groups = groupsAttribute.getAttribute(attributes);
        final String dn = resolveSingleValueAttribute(attributes, dnAttribute, DN_ATTRIBUTE.name(config));
        final String name = resolveSingleValueAttribute(attributes, nameAttribute, NAME_ATTRIBUTE.name(config));
        final String mail = resolveSingleValueAttribute(attributes, mailAttribute, MAIL_ATTRIBUTE.name(config));
        UserRoleMapper.UserData userData = new UserRoleMapper.UserData(principal, dn, groups, userMeta, config);
        logger.debug("SAML attribute mapping = [{}]", userData);
        roleMapper.resolveRoles(userData, wrappedListener.delegateFailureAndWrap((l, roles) -> {
            final User user = new User(principal, roles.toArray(new String[roles.size()]), name, mail, userMeta, true);
            logger.debug("SAML user = [{}]", user);
            l.onResponse(AuthenticationResult.success(user));
        }));
    }

    public Map<String, Object> createTokenMetadata(SamlNameId nameId, String session) {
        final Map<String, Object> tokenMeta = new HashMap<>();
        if (nameId != null) {
            tokenMeta.put(TOKEN_METADATA_NAMEID_VALUE, nameId.value);
            tokenMeta.put(TOKEN_METADATA_NAMEID_FORMAT, nameId.format);
            tokenMeta.put(TOKEN_METADATA_NAMEID_QUALIFIER, nameId.idpNameQualifier);
            tokenMeta.put(TOKEN_METADATA_NAMEID_SP_QUALIFIER, nameId.spNameQualifier);
            tokenMeta.put(TOKEN_METADATA_NAMEID_SP_PROVIDED_ID, nameId.spProvidedId);
        } else {
            tokenMeta.put(TOKEN_METADATA_NAMEID_VALUE, null);
            tokenMeta.put(TOKEN_METADATA_NAMEID_FORMAT, null);
            tokenMeta.put(TOKEN_METADATA_NAMEID_QUALIFIER, null);
            tokenMeta.put(TOKEN_METADATA_NAMEID_SP_QUALIFIER, null);
            tokenMeta.put(TOKEN_METADATA_NAMEID_SP_PROVIDED_ID, null);
        }
        tokenMeta.put(TOKEN_METADATA_SESSION, session);
        tokenMeta.put(TOKEN_METADATA_REALM, name());
        return tokenMeta;
    }

    private static String resolveSingleValueAttribute(SamlAttributes attributes, AttributeParser parser, String name) {
        final List<String> list = parser.getAttribute(attributes);
        switch (list.size()) {
            case 0:
                return null;
            case 1:
                return list.get(0);
            default:
                logger.info("SAML assertion contains multiple values for attribute [{}] returning first one", name);
                return list.get(0);
        }
    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {
        // saml will not support user lookup initially
        listener.onResponse(null);
    }

    static Tuple<AbstractReloadingMetadataResolver, Supplier<EntityDescriptor>> initializeResolver(
        Logger logger,
        RealmConfig config,
        SSLService sslService,
        ResourceWatcherService watcherService
    ) throws ResolverException, ComponentInitializationException, PrivilegedActionException, IOException {
        final String metadataUrl = require(config, IDP_METADATA_PATH);
        if (metadataUrl.startsWith("http://")) {
            throw new IllegalArgumentException("The [http] protocol is not supported as it is insecure. Use [https] instead");
        } else if (metadataUrl.startsWith("https://")) {
            return parseHttpMetadata(metadataUrl, config, sslService);
        } else {
            return parseFileSystemMetadata(logger, metadataUrl, config, watcherService);
        }
    }

    private static Tuple<AbstractReloadingMetadataResolver, Supplier<EntityDescriptor>> parseHttpMetadata(
        String metadataUrl,
        RealmConfig config,
        SSLService sslService
    ) throws ResolverException, ComponentInitializationException, PrivilegedActionException {
        final String entityId = require(config, IDP_ENTITY_ID);

        HttpClientBuilder builder = HttpClientBuilder.create();
        // ssl setup
        final String sslKey = RealmSettings.realmSslPrefix(config.identifier());
        final SslConfiguration sslConfiguration = sslService.getSSLConfiguration(sslKey);
        final HostnameVerifier verifier = SSLService.getHostnameVerifier(sslConfiguration);
        SSLConnectionSocketFactory factory = new SSLConnectionSocketFactory(sslService.sslSocketFactory(sslConfiguration), verifier);
        builder.setSSLSocketFactory(factory);

        TimeValue maxRefresh = config.getSetting(IDP_METADATA_HTTP_REFRESH);
        TimeValue minRefresh = config.getSetting(IDP_METADATA_HTTP_MIN_REFRESH);
        if (minRefresh.compareTo(maxRefresh) > 0) {
            if (config.hasSetting(IDP_METADATA_HTTP_MIN_REFRESH)) {
                throw new SettingsException(
                    "the value ({}) for [{}] cannot be greater than the value ({}) for [{}]",
                    minRefresh.getStringRep(),
                    RealmSettings.getFullSettingKey(config, IDP_METADATA_HTTP_MIN_REFRESH),
                    maxRefresh.getStringRep(),
                    RealmSettings.getFullSettingKey(config, IDP_METADATA_HTTP_REFRESH)
                );
            } else {
                minRefresh = maxRefresh;
            }
        }

        HTTPMetadataResolver resolver = new PrivilegedHTTPMetadataResolver(builder.build(), metadataUrl);
        resolver.setMinRefreshDelay(Duration.ofMillis(minRefresh.millis()));
        resolver.setMaxRefreshDelay(Duration.ofMillis(maxRefresh.millis()));

        final boolean failOnError = config.getSetting(IDP_METADATA_HTTP_FAIL_ON_ERROR);
        resolver.setFailFastInitialization(failOnError);

        initialiseResolver(resolver, config);

        return new Tuple<>(resolver, () -> {
            // for some reason the resolver supports its own trust engine and custom socket factories.
            // we do not use these as we'd rather rely on the JDK versions for TLS security!
            SpecialPermission.check();
            try {
                return AccessController.doPrivileged(
                    (PrivilegedExceptionAction<EntityDescriptor>) () -> resolveEntityDescriptor(
                        resolver,
                        entityId,
                        metadataUrl,
                        failOnError
                    )
                );
            } catch (PrivilegedActionException e) {
                throw ExceptionsHelper.convertToRuntime((Exception) ExceptionsHelper.unwrapCause(e));
            }
        });
    }

    private static final class PrivilegedHTTPMetadataResolver extends HTTPMetadataResolver {

        PrivilegedHTTPMetadataResolver(final HttpClient client, final String metadataURL) throws ResolverException {
            super(client, metadataURL);
        }

        @Override
        protected byte[] fetchMetadata() throws ResolverException {
            assert assertNotTransportThread("fetching SAML metadata from a URL");
            try {
                return AccessController.doPrivileged(
                    (PrivilegedExceptionAction<byte[]>) () -> PrivilegedHTTPMetadataResolver.super.fetchMetadata()
                );
            } catch (final PrivilegedActionException e) {
                throw (ResolverException) e.getCause();
            }
        }

    }

    @SuppressForbidden(reason = "uses java.io.File")
    private static final class SamlFilesystemMetadataResolver extends FilesystemMetadataResolver {

        SamlFilesystemMetadataResolver(final java.io.File metadata) throws ResolverException {
            super(metadata);
        }

        @Override
        protected byte[] fetchMetadata() throws ResolverException {
            assert assertNotTransportThread("fetching SAML metadata from a file");
            try {
                return AccessController.doPrivileged((PrivilegedExceptionAction<byte[]>) () -> super.fetchMetadata());
            } catch (PrivilegedActionException e) {
                throw (ResolverException) e.getException();
            }
        }
    }

    @SuppressForbidden(reason = "uses toFile")
    private static Tuple<AbstractReloadingMetadataResolver, Supplier<EntityDescriptor>> parseFileSystemMetadata(
        Logger logger,
        String metadataPath,
        RealmConfig config,
        ResourceWatcherService watcherService
    ) throws ResolverException, ComponentInitializationException, IOException, PrivilegedActionException {

        final String entityId = require(config, IDP_ENTITY_ID);
        final Path path = config.env().configFile().resolve(metadataPath);
        final FilesystemMetadataResolver resolver = new SamlFilesystemMetadataResolver(path.toFile());

        for (var httpSetting : List.of(IDP_METADATA_HTTP_REFRESH, IDP_METADATA_HTTP_MIN_REFRESH, IDP_METADATA_HTTP_FAIL_ON_ERROR)) {
            if (config.hasSetting(httpSetting)) {
                logger.info(
                    "Ignoring setting [{}] because the IdP metadata is being loaded from a file",
                    RealmSettings.getFullSettingKey(config, httpSetting)
                );
            }
        }

        // We don't want to rely on the internal OpenSAML refresh timer, but we can't turn it off, so just set it to run once a day.
        // @TODO : Submit a patch to OpenSAML to optionally disable the timer
        final Duration oneDayMs = Duration.ofMillis(TimeValue.timeValueHours(24).millis());
        resolver.setMinRefreshDelay(oneDayMs);
        resolver.setMaxRefreshDelay(oneDayMs);
        initialiseResolver(resolver, config);

        FileWatcher watcher = new PrivilegedFileWatcher(path);
        watcher.addListener(new FileListener(logger, resolver::refresh));
        watcherService.add(watcher, ResourceWatcherService.Frequency.MEDIUM);
        return new Tuple<>(resolver, () -> resolveEntityDescriptor(resolver, entityId, path.toString(), true));
    }

    private static EntityDescriptor resolveEntityDescriptor(
        AbstractReloadingMetadataResolver resolver,
        String entityId,
        String sourceLocation,
        boolean failOnError
    ) {
        try {
            final EntityDescriptor descriptor = resolveEntityDescriptorWithPossibleRefresh(resolver, entityId);
            if (descriptor == null) {
                if (failOnError) {
                    throw SamlUtils.samlException("Cannot find metadata for entity [{}] in [{}]", entityId, sourceLocation);
                } else {
                    logger.warn(
                        "cannot load SAML metadata for [{}] from [{}]; SAML authentication for this realm will fail",
                        entityId,
                        sourceLocation
                    );
                    return new UnresolvedEntity(entityId, sourceLocation);
                }
            }
            return descriptor;
        } catch (ResolverException e) {
            throw SamlUtils.samlException("Cannot resolve entity metadata", e);
        }
    }

    private static EntityDescriptor resolveEntityDescriptorWithPossibleRefresh(AbstractReloadingMetadataResolver resolver, String entityId)
        throws ResolverException {
        var criteria = new CriteriaSet(new EntityIdCriterion(entityId));
        EntityDescriptor descriptor = resolver.resolveSingle(criteria);
        if (descriptor == null) {
            /*
             * If the descriptor is null, we haven't found a metadata file with that entity id in it. This could be caused by 2 things:
             * 1. We didn't find any metadata file (e.g. the metadata is loaded over http, and the request returned an error
             * 2. The file does exist, but doesn't contain the entity.
             * In either case it's worth refreshing the metadata again to see if it's now correct because it's possible that the problem
             *    has been resolved (although if metadata is loaded from a local file we monitor it for changes anyway, so this refresh
             *    is unlikely to have any benefit).
             * The resolver's refresh method is synchronized, and could be a choke point if there are a lot of concurrent login attempts,
             *    so we wrap it in a check to only call it on 1 thread. The other threads will fail due to bad metadata, but that's OK
             *    because the metadata is (was) bad, and if the refresh works, then their next login attempt will also work.
             * Since the dynamic refresh is a saving mechanism (we have a separate regular poll for refreshes) we prefer it to be safe
             *    (from a thread contention point of view) rather than perfect (from a user experience point of view).
             */
            if (REFRESHING_METADATA.compareAndSet(false, true)) {
                try {
                    resolver.refresh();
                } catch (ResolverException e) {
                    // Refresh failed. Since there could be a lot of these and it's only a saving mechanism we log as debug
                    // The resolver itself will log a WARN with the HTTP status etc.
                    logger.debug(() -> "Failed to refresh SAML metadata for [" + entityId + "]", e);
                } finally {
                    REFRESHING_METADATA.compareAndSet(true, false);
                }
            }
            descriptor = resolver.resolveSingle(criteria);
            if (descriptor != null) {
                logger.debug(() -> "SAML metadata for [" + entityId + "] has been successfully refreshed");
            }
        }
        return descriptor;
    }

    @Override
    public void close() {
        Releasables.close(releasables);
    }

    private static void initialiseResolver(AbstractReloadingMetadataResolver resolver, RealmConfig config)
        throws ComponentInitializationException, PrivilegedActionException {
        resolver.setRequireValidMetadata(true);
        BasicParserPool pool = new BasicParserPool();
        pool.initialize();
        resolver.setParserPool(pool);
        resolver.setId(config.name());
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedExceptionAction<Object>) () -> {
            try {
                resolver.initialize();
            } catch (ComponentInitializationException e) {
                resolver.destroy();
                throw SamlUtils.samlException("cannot load SAML metadata from [{}]", e, config.getSetting(IDP_METADATA_PATH));
            }
            return null;
        });
    }

    public String serviceProviderEntityId() {
        return this.serviceProvider.getEntityId();
    }

    public String assertionConsumerServiceURL() {
        return this.serviceProvider.getAscUrl();
    }

    public AuthnRequest buildAuthenticationRequest() {
        final AuthnRequest authnRequest = new SamlAuthnRequestBuilder(
            serviceProvider,
            SAMLConstants.SAML2_POST_BINDING_URI,
            idpDescriptor.get(),
            SAMLConstants.SAML2_REDIRECT_BINDING_URI,
            Clock.systemUTC()
        ).nameIDPolicy(nameIdPolicy).forceAuthn(forceAuthn).build();
        if (logger.isTraceEnabled()) {
            logger.trace("Constructed SAML Authentication Request: {}", SamlUtils.getXmlContent(authnRequest, true));
        }
        return authnRequest;
    }

    /**
     * Creates a SAML {@link LogoutRequest Single LogOut request} for the provided session, if the
     * realm and IdP configuration support SLO. Otherwise returns {@code null}
     *
     * @see SamlRealmSettings#IDP_SINGLE_LOGOUT
     */
    public LogoutRequest buildLogoutRequest(NameID nameId, String session) {
        if (useSingleLogout) {
            final LogoutRequest logoutRequest = new SamlLogoutRequestMessageBuilder(
                Clock.systemUTC(),
                serviceProvider,
                idpDescriptor.get(),
                nameId,
                session
            ).build();
            if (logoutRequest != null && logger.isTraceEnabled()) {
                logger.trace("Constructed SAML Logout Request: {}", SamlUtils.getXmlContent(logoutRequest, true));
            }
            return logoutRequest;
        } else {
            return null;
        }

    }

    /**
     * Creates a SAML {@link org.opensaml.saml.saml2.core.LogoutResponse} to the provided requestID
     */
    public LogoutResponse buildLogoutResponse(String inResponseTo) {
        final LogoutResponse logoutResponse = new SamlLogoutResponseBuilder(
            Clock.systemUTC(),
            serviceProvider,
            idpDescriptor.get(),
            inResponseTo,
            StatusCode.SUCCESS
        ).build();
        if (logoutResponse != null && logger.isTraceEnabled()) {
            logger.trace("Constructed SAML Logout Response: {}", SamlUtils.getXmlContent(logoutResponse, true));
        }
        return logoutResponse;
    }

    public SigningConfiguration getSigningConfiguration() {
        return serviceProvider.getSigningConfiguration();
    }

    public SamlLogoutRequestHandler getLogoutHandler() {
        return this.logoutHandler;
    }

    public SamlLogoutResponseHandler getLogoutResponseHandler() {
        return logoutResponseHandler;
    }

    private static class FileListener implements FileChangesListener {

        private final Logger logger;
        private final CheckedRunnable<Exception> onChange;

        private FileListener(Logger logger, CheckedRunnable<Exception> onChange) {
            this.logger = logger;
            this.onChange = onChange;
        }

        @Override
        public void onFileCreated(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileDeleted(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileChanged(Path file) {
            try {
                onChange.run();
            } catch (Exception e) {
                logger.warn(() -> "An error occurred while reloading file [" + file + "]", e);
            }
        }
    }

    static final class AttributeParser {
        private final String name;
        private final Function<SamlAttributes, List<String>> parser;

        AttributeParser(String name, Function<SamlAttributes, List<String>> parser) {
            this.name = name;
            this.parser = parser;
        }

        List<String> getAttribute(SamlAttributes attributes) {
            final List<String> attrValue = parser.apply(attributes);
            logger.trace(() -> format("Parser [%s] generated values [%s]", name, collectionToCommaDelimitedString(attrValue)));
            return attrValue;
        }

        @Override
        public String toString() {
            return name;
        }

        static AttributeParser forSetting(Logger logger, SamlRealmSettings.AttributeSettingWithDelimiter setting, RealmConfig realmConfig) {
            SamlRealmSettings.AttributeSetting attributeSetting = setting.getAttributeSetting();
            if (realmConfig.hasSetting(setting.getDelimiter())) {
                if (realmConfig.hasSetting(attributeSetting.getAttribute()) == false) {
                    throw new SettingsException(
                        "Setting ["
                            + RealmSettings.getFullSettingKey(realmConfig, setting.getDelimiter())
                            + "] cannot be set unless ["
                            + RealmSettings.getFullSettingKey(realmConfig, attributeSetting.getAttribute())
                            + "] is also set"
                    );
                }
                if (realmConfig.hasSetting(attributeSetting.getPattern())) {
                    throw new SettingsException(
                        "Setting ["
                            + RealmSettings.getFullSettingKey(realmConfig, attributeSetting.getPattern())
                            + "] can not be set when ["
                            + RealmSettings.getFullSettingKey(realmConfig, setting.getDelimiter())
                            + "] is set"
                    );
                }

                String attributeName = realmConfig.getSetting(attributeSetting.getAttribute());
                String delimiter = realmConfig.getSetting(setting.getDelimiter());
                return new AttributeParser(
                    "SAML Attribute ["
                        + attributeName
                        + "] with delimiter ["
                        + delimiter
                        + "] for ["
                        + attributeSetting.name(realmConfig)
                        + "]",
                    attributes -> {
                        List<String> attributeValues = attributes.getAttributeValues(attributeName);
                        if (attributeValues.size() > 1) {
                            throw SamlUtils.samlException(
                                "Expected single string value for attribute: ["
                                    + attributeName
                                    + "], but got list with "
                                    + attributeValues.size()
                                    + " values"
                            );
                        }
                        return attributeValues.stream()
                            .map(s -> s.split(Pattern.quote(delimiter)))
                            .flatMap(Arrays::stream)
                            .filter(attribute -> {
                                if (Strings.isNullOrEmpty(attribute)) {
                                    logger.debug("Attribute [{}] has empty components when using delimiter [{}]", attributeName, delimiter);
                                    return false;
                                }
                                return true;
                            })
                            .collect(Collectors.toList());
                    }
                );
            }
            return AttributeParser.forSetting(logger, attributeSetting, realmConfig, false);
        }

        static AttributeParser forSetting(
            Logger logger,
            SamlRealmSettings.AttributeSetting setting,
            RealmConfig realmConfig,
            boolean required
        ) {
            if (realmConfig.hasSetting(setting.getAttribute())) {
                String attributeName = realmConfig.getSetting(setting.getAttribute());
                if (realmConfig.hasSetting(setting.getPattern())) {
                    Pattern regex = Pattern.compile(realmConfig.getSetting(setting.getPattern()));
                    return new AttributeParser(
                        "SAML Attribute ["
                            + attributeName
                            + "] with pattern ["
                            + regex.pattern()
                            + "] for ["
                            + setting.name(realmConfig)
                            + "]",
                        attributes -> attributes.getAttributeValues(attributeName).stream().map(s -> {
                            final Matcher matcher = regex.matcher(s);
                            if (matcher.find() == false) {
                                logger.debug("Attribute [{}] is [{}], which does not match [{}]", attributeName, s, regex.pattern());
                                return null;
                            }
                            final String value = matcher.group(1);
                            if (Strings.isNullOrEmpty(value)) {
                                logger.debug(
                                    "Attribute [{}] is [{}], which does match [{}] but group(1) is empty",
                                    attributeName,
                                    s,
                                    regex.pattern()
                                );
                                return null;
                            }
                            return value;
                        }).filter(Objects::nonNull).toList()
                    );
                } else {
                    return new AttributeParser(
                        "SAML Attribute [" + attributeName + "] for [" + setting.name(realmConfig) + "]",
                        attributes -> attributes.getAttributeValues(attributeName)
                    );
                }
            } else if (required) {
                throw new SettingsException(
                    "Setting [" + RealmSettings.getFullSettingKey(realmConfig, setting.getAttribute()) + "] is required"
                );
            } else if (realmConfig.hasSetting(setting.getPattern())) {
                throw new SettingsException(
                    "Setting ["
                        + RealmSettings.getFullSettingKey(realmConfig, setting.getPattern())
                        + "] cannot be set unless ["
                        + RealmSettings.getFullSettingKey(realmConfig, setting.getAttribute())
                        + "] is also set"
                );
            } else {
                return new AttributeParser("No SAML attribute for [" + setting.name(realmConfig) + "]", attributes -> List.of());
            }
        }

    }
}
