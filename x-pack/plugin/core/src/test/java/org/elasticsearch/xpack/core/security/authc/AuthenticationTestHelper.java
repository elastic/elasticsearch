/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SecurityProfileUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.UsernamesField;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class helps to randomize {@link Authentication} related objects. It provides methods
 * to randomize {@link User}, {@link Authentication.RealmRef}, {@link RealmDomain} and a builder
 * to randomize {@link Authentication}. It should be used anytime a randomized Authentication
 * object is needed in tests to ensure the Authentication object is created correctly, i.e. satisfies
 * its internal logic.
 *
 * The Authentication builder class provides configuration methods that should suffice most customisations.
 * There are dedicate methods for creating specific type of Authentication, e.g. {@code builder().apiKey().build()}.
 *
 * The simplest way to get a completely random Authentication that cover most (if not all) possible scenarios is to
 * simply call {@code builder().build()}.
 */
public class AuthenticationTestHelper {

    private static final Set<String> SYNTHETIC_REALM_TYPES = Set.of(
        AuthenticationField.API_KEY_REALM_TYPE,
        AuthenticationField.ANONYMOUS_REALM_TYPE,
        AuthenticationField.ATTACH_REALM_TYPE,
        AuthenticationField.FALLBACK_REALM_TYPE,
        ServiceAccountSettings.REALM_TYPE
    );

    private static final Set<User> INTERNAL_USERS = Set.of(
        SystemUser.INSTANCE,
        XPackUser.INSTANCE,
        XPackSecurityUser.INSTANCE,
        AsyncSearchUser.INSTANCE,
        SecurityProfileUser.INSTANCE
    );

    public static AuthenticationTestBuilder builder() {
        return new AuthenticationTestBuilder();
    }

    public static User randomUser() {
        return new User(
            ESTestCase.randomAlphaOfLengthBetween(3, 8),
            ESTestCase.randomArray(1, 3, String[]::new, () -> ESTestCase.randomAlphaOfLengthBetween(3, 8))
        );
    }

    public static User userWithRandomMetadataAndDetails(final String username, final String... roles) {
        return new User(
            username,
            roles,
            ESTestCase.randomFrom(ESTestCase.randomAlphaOfLengthBetween(1, 10), null),
            // Not a very realistic email address, but we don't validate this nor rely on correct format, so keeping it simple
            ESTestCase.randomFrom(ESTestCase.randomAlphaOfLengthBetween(1, 10), null),
            randomUserMetadata(),
            true
        );
    }

    public static Map<String, Object> randomUserMetadata() {
        return ESTestCase.randomFrom(
            Map.of(
                "employee_id",
                ESTestCase.randomAlphaOfLength(5),
                "number",
                1,
                "numbers",
                List.of(1, 3, 5),
                "extra",
                Map.of("favorite pizza", "hawaii", "age", 42)
            ),
            Map.of(ESTestCase.randomAlphaOfLengthBetween(3, 8), ESTestCase.randomAlphaOfLengthBetween(3, 8)),
            Map.of(),
            null
        );
    }

    public static RealmDomain randomDomain(boolean includeInternal) {
        final Supplier<String> randomRealmTypeSupplier = randomRealmTypeSupplier(includeInternal);
        final Set<RealmConfig.RealmIdentifier> domainRealms = new HashSet<>(
            Arrays.asList(
                ESTestCase.randomArray(
                    1,
                    4,
                    RealmConfig.RealmIdentifier[]::new,
                    () -> new RealmConfig.RealmIdentifier(
                        randomRealmTypeSupplier.get(),
                        ESTestCase.randomAlphaOfLengthBetween(3, 8).toLowerCase(Locale.ROOT)
                    )
                )
            )
        );
        return new RealmDomain(ESTestCase.randomAlphaOfLengthBetween(3, 8), domainRealms);
    }

    public static Authentication.RealmRef randomRealmRef() {
        return randomRealmRef(ESTestCase.randomBoolean());
    }

    public static Authentication.RealmRef randomRealmRef(boolean underDomain) {
        return randomRealmRef(underDomain, true);
    }

    public static Authentication.RealmRef randomRealmRef(boolean underDomain, boolean includeInternal) {
        if (underDomain) {
            RealmDomain domain = randomDomain(includeInternal);
            RealmConfig.RealmIdentifier realmIdentifier = ESTestCase.randomFrom(domain.realms());
            return new Authentication.RealmRef(
                realmIdentifier.getName(),
                realmIdentifier.getType(),
                ESTestCase.randomAlphaOfLengthBetween(3, 8),
                domain
            );
        } else {
            return new Authentication.RealmRef(
                ESTestCase.randomAlphaOfLengthBetween(3, 8),
                randomRealmTypeSupplier(includeInternal).get(),
                ESTestCase.randomAlphaOfLengthBetween(3, 8),
                null
            );
        }
    }

    public static RealmConfig.RealmIdentifier randomRealmIdentifier(boolean includeInternal) {
        return new RealmConfig.RealmIdentifier(randomRealmTypeSupplier(includeInternal).get(), ESTestCase.randomAlphaOfLengthBetween(3, 8));
    }

    private static Supplier<String> randomRealmTypeSupplier(boolean includeInternal) {
        final Supplier<String> randomAllRealmTypeSupplier = () -> ESTestCase.randomFrom(
            "reserved",
            FileRealmSettings.TYPE,
            NativeRealmSettings.TYPE,
            LdapRealmSettings.AD_TYPE,
            LdapRealmSettings.LDAP_TYPE,
            JwtRealmSettings.TYPE,
            OpenIdConnectRealmSettings.TYPE,
            SamlRealmSettings.TYPE,
            KerberosRealmSettings.TYPE,
            PkiRealmSettings.TYPE,
            ESTestCase.randomAlphaOfLengthBetween(3, 8)
        );
        if (includeInternal) {
            return randomAllRealmTypeSupplier;
        } else {
            return () -> ESTestCase.randomValueOtherThanMany(
                value -> value.equals(FileRealmSettings.TYPE) || value.equals(NativeRealmSettings.TYPE) || value.equals("reserved"),
                randomAllRealmTypeSupplier
            );
        }
    }

    private static AnonymousUser randomAnonymousUser() {
        return new AnonymousUser(
            Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), ESTestCase.randomAlphaOfLengthBetween(3, 8)).build()
        );
    }

    private static User stripRoles(User user) {
        if (user.roles() != null || user.roles().length == 0) {
            return new User(user.principal(), Strings.EMPTY_ARRAY, user.fullName(), user.email(), user.metadata(), user.enabled());
        } else {
            return user;
        }
    }

    public static String randomInternalUsername() {
        return builder().internal().build(false).getUser().principal();
    }

    /**
     * @return non-empty collection of internal usernames
     */
    public static List<String> randomInternalUsernames() {
        return ESTestCase.randomNonEmptySubsetOf(INTERNAL_USERS.stream().map(User::principal).toList());
    }

    public static String randomInternalRoleName() {
        return ESTestCase.randomFrom(
            UsernamesField.SYSTEM_ROLE,
            UsernamesField.XPACK_ROLE,
            UsernamesField.ASYNC_SEARCH_ROLE,
            UsernamesField.XPACK_SECURITY_ROLE,
            UsernamesField.SECURITY_PROFILE_ROLE
        );
    }

    public static class AuthenticationTestBuilder {
        private Version version;
        private Authentication authenticatingAuthentication;
        private User user;
        private Authentication.RealmRef realmRef;
        private EnumSet<AuthenticationType> candidateAuthenticationTypes = EnumSet.allOf(AuthenticationType.class);
        private final Map<String, Object> metadata = new HashMap<>();
        private Boolean isServiceAccount;
        private Boolean isRealmUnderDomain;

        private AuthenticationTestBuilder() {}

        private AuthenticationTestBuilder(Authentication authentication) {
            assert false == authentication.isRunAs() : "authenticating authentication cannot itself be run-as";
            this.authenticatingAuthentication = authentication;
            this.version = authentication.getVersion();
        }

        public AuthenticationTestBuilder realm() {
            return realm(ESTestCase.randomBoolean());
        }

        public AuthenticationTestBuilder realm(boolean underDomain) {
            assert authenticatingAuthentication == null : "shortcut method cannot be used for effective authentication";
            resetShortcutRelatedVariables();
            this.isRealmUnderDomain = underDomain;
            user = null;
            realmRef = null;
            candidateAuthenticationTypes = EnumSet.of(AuthenticationType.REALM);
            return this;
        }

        public AuthenticationTestBuilder serviceAccount() {
            return serviceAccount(
                new User(ESTestCase.randomAlphaOfLengthBetween(3, 8) + "/" + ESTestCase.randomAlphaOfLengthBetween(3, 8))
            );
        }

        public AuthenticationTestBuilder serviceAccount(User user) {
            assert authenticatingAuthentication == null : "shortcut method cannot be used for effective authentication";
            assert user.principal().contains("/") : "invalid service account principal";
            resetShortcutRelatedVariables();
            this.user = user;
            isServiceAccount = true;
            realmRef = null;
            candidateAuthenticationTypes = EnumSet.of(AuthenticationType.TOKEN);
            return this;
        }

        public AuthenticationTestBuilder apiKey() {
            return apiKey(ESTestCase.randomAlphaOfLength(20));
        }

        public AuthenticationTestBuilder apiKey(String apiKeyId) {
            assert authenticatingAuthentication == null : "shortcut method cannot be used for effective authentication";
            resetShortcutRelatedVariables();
            realmRef = null;
            candidateAuthenticationTypes = EnumSet.of(AuthenticationType.API_KEY);
            metadata.put(AuthenticationField.API_KEY_ID_KEY, Objects.requireNonNull(apiKeyId));
            return this;
        }

        public AuthenticationTestBuilder anonymous() {
            return anonymous(randomAnonymousUser());
        }

        public AuthenticationTestBuilder anonymous(User user) {
            assert authenticatingAuthentication == null : "shortcut method cannot be used for effective authentication";
            assert user instanceof AnonymousUser : "user must be anonymous for anonymous authentication";
            resetShortcutRelatedVariables();
            this.user = user;
            realmRef = null;
            candidateAuthenticationTypes = EnumSet.of(AuthenticationType.ANONYMOUS);
            return this;
        }

        public AuthenticationTestBuilder internal() {
            return internal(ESTestCase.randomFrom(INTERNAL_USERS));
        }

        public AuthenticationTestBuilder internal(User user) {
            assert authenticatingAuthentication == null : "shortcut method cannot be used for effective authentication";
            assert User.isInternal(user) : "user must be internal for internal authentication";
            resetShortcutRelatedVariables();
            this.user = user;
            realmRef = null;
            candidateAuthenticationTypes = EnumSet.of(AuthenticationType.INTERNAL);
            return this;
        }

        public AuthenticationTestBuilder user(User user) {
            if (User.isInternal(user)) {
                return internal(user);
            } else if (user instanceof AnonymousUser) {
                return anonymous(user);
            } else {
                this.user = user;
                candidateAuthenticationTypes.removeIf(t -> t == AuthenticationType.INTERNAL || t == AuthenticationType.ANONYMOUS);
                return this;
            }
        }

        public AuthenticationTestBuilder realmRef(Authentication.RealmRef realmRef) {
            assert false == SYNTHETIC_REALM_TYPES.contains(realmRef.getType()) : "use dedicate methods for synthetic realms";
            resetShortcutRelatedVariables();
            this.realmRef = realmRef;
            isServiceAccount = false;
            candidateAuthenticationTypes.removeIf(
                t -> t == AuthenticationType.INTERNAL || t == AuthenticationType.ANONYMOUS || t == AuthenticationType.API_KEY
            );
            return this;
        }

        public AuthenticationTestBuilder version(Version version) {
            if (authenticatingAuthentication != null) {
                throw new IllegalArgumentException("cannot set version for run-as authentication");
            }
            this.version = Objects.requireNonNull(version);
            return this;
        }

        public AuthenticationTestBuilder metadata(Map<String, Object> metadata) {
            if (authenticatingAuthentication != null) {
                throw new IllegalArgumentException("cannot add metadata for run-as authentication");
            }
            this.metadata.putAll(Objects.requireNonNull(metadata));
            return this;
        }

        public AuthenticationTestBuilder runAs() {
            if (authenticatingAuthentication != null) {
                throw new IllegalArgumentException("cannot convert to run-as again for run-as authentication");
            }
            candidateAuthenticationTypes = candidateAuthenticationTypes.stream()
                .filter(t -> t == AuthenticationType.REALM || t == AuthenticationType.API_KEY)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(AuthenticationType.class)));
            final Authentication authentication = build(false);
            return new AuthenticationTestBuilder(authentication);
        }

        public Authentication build() {
            return build(ESTestCase.randomBoolean());
        }

        public Authentication build(boolean runAsIfNotAlready) {
            if (authenticatingAuthentication != null) {
                if (user == null) {
                    user = randomUser();
                }
                assert false == User.isInternal(user) && false == user instanceof AnonymousUser
                    : "cannot run-as internal or anonymous user";
                if (realmRef == null) {
                    realmRef = randomRealmRef(isRealmUnderDomain == null ? ESTestCase.randomBoolean() : isRealmUnderDomain);
                }
                assert false == SYNTHETIC_REALM_TYPES.contains(realmRef.getType()) : "cannot run-as users from synthetic realms";
                return authenticatingAuthentication.runAs(user, realmRef);
            } else {
                assert candidateAuthenticationTypes.size() > 0 : "no candidate authentication types";
                final Authentication authentication;
                final AuthenticationType authenticationType = ESTestCase.randomFrom(candidateAuthenticationTypes);
                switch (authenticationType) {
                    case REALM -> {
                        if (user == null) {
                            user = randomUser();
                        }
                        if (realmRef == null) {
                            realmRef = randomRealmRef(isRealmUnderDomain == null ? ESTestCase.randomBoolean() : isRealmUnderDomain);
                        }
                        assert false == SYNTHETIC_REALM_TYPES.contains(realmRef.getType()) : "use dedicate methods for synthetic realms";
                        if (runAsIfNotAlready) {
                            authentication = builder().runAs().user(user).realmRef(realmRef).build();
                        } else {
                            authentication = Authentication.newRealmAuthentication(user, realmRef);
                        }
                    }
                    case API_KEY -> {
                        assert realmRef == null : "cannot specify realm type for API key authentication";
                        if (user == null) {
                            user = randomUser();
                        }
                        // User associated to API key authentication has empty roles
                        user = stripRoles(user);
                        prepareApiKeyMetadata();
                        authentication = Authentication.newApiKeyAuthentication(
                            AuthenticationResult.success(user, metadata),
                            ESTestCase.randomAlphaOfLengthBetween(3, 8)
                        );
                    }
                    case TOKEN -> {
                        if (isServiceAccount != null && isServiceAccount) {
                            // service account
                            assert user != null && user.principal().contains("/") : "invalid service account principal";
                            assert realmRef == null : "cannot specify realm type for service account authentication";
                            prepareServiceAccountMetadata();
                            authentication = Authentication.newServiceAccountAuthentication(
                                user,
                                ESTestCase.randomAlphaOfLengthBetween(3, 8),
                                metadata
                            );
                        } else {
                            final int tokenVariant = ESTestCase.randomIntBetween(0, 9);
                            if (tokenVariant == 0 && user == null && realmRef == null) {
                                // service account
                                prepareServiceAccountMetadata();
                                authentication = Authentication.newServiceAccountAuthentication(
                                    new User(
                                        ESTestCase.randomAlphaOfLengthBetween(3, 8) + "/" + ESTestCase.randomAlphaOfLengthBetween(3, 8)
                                    ),
                                    ESTestCase.randomAlphaOfLengthBetween(3, 8),
                                    metadata
                                );
                            } else if (tokenVariant == 1 && realmRef == null) {
                                // token by api key
                                if (user == null) {
                                    user = randomUser();
                                }
                                // User associated to API key authentication has empty roles
                                user = stripRoles(user);
                                prepareApiKeyMetadata();
                                authentication = Authentication.newApiKeyAuthentication(
                                    AuthenticationResult.success(user, metadata),
                                    ESTestCase.randomAlphaOfLengthBetween(3, 8)
                                ).token();
                            } else if (tokenVariant == 2 && user == null && realmRef == null) {
                                // token by anonymous user
                                authentication = Authentication.newAnonymousAuthentication(
                                    randomAnonymousUser(),
                                    ESTestCase.randomAlphaOfLengthBetween(3, 8)
                                ).token();
                            } else {
                                // token by realm user
                                if (user == null) {
                                    user = randomUser();
                                }
                                if (realmRef == null) {
                                    realmRef = randomRealmRef(isRealmUnderDomain == null ? ESTestCase.randomBoolean() : isRealmUnderDomain);
                                }
                                authentication = Authentication.newRealmAuthentication(user, realmRef).token();
                            }
                        }
                    }
                    case ANONYMOUS -> {
                        if (user == null) {
                            user = randomAnonymousUser();
                        }
                        assert user instanceof AnonymousUser : "user must be anonymous for anonymous authentication";
                        assert realmRef == null : "cannot specify realm type for anonymous authentication";
                        authentication = Authentication.newAnonymousAuthentication(
                            (AnonymousUser) user,
                            ESTestCase.randomAlphaOfLengthBetween(3, 8)
                        );
                    }
                    case INTERNAL -> {
                        if (user == null) {
                            user = ESTestCase.randomFrom(INTERNAL_USERS);
                        }
                        assert User.isInternal(user) : "user must be internal for internal authentication";
                        assert realmRef == null : "cannot specify realm type for internal authentication";
                        String nodeName = ESTestCase.randomAlphaOfLengthBetween(3, 8);
                        if (user == SystemUser.INSTANCE) {
                            authentication = ESTestCase.randomFrom(
                                Authentication.newInternalAuthentication(user, Version.CURRENT, nodeName),
                                Authentication.newInternalFallbackAuthentication(user, nodeName)
                            );
                        } else {
                            authentication = Authentication.newInternalAuthentication(user, Version.CURRENT, nodeName);
                        }
                    }
                    default -> throw new IllegalArgumentException("unknown authentication type [" + authenticationType + "]");
                }
                if (version == null) {
                    version = Version.CURRENT;
                }
                if (version.before(authentication.getVersion())) {
                    return authentication.maybeRewriteForOlderVersion(version);
                } else {
                    return authentication;
                }
            }
        }

        private void prepareApiKeyMetadata() {
            if (false == metadata.containsKey(AuthenticationField.API_KEY_ID_KEY)) {
                metadata.put(AuthenticationField.API_KEY_ID_KEY, ESTestCase.randomAlphaOfLength(20));
            }
            if (false == metadata.containsKey(AuthenticationField.API_KEY_NAME_KEY)) {
                metadata.put(
                    AuthenticationField.API_KEY_NAME_KEY,
                    ESTestCase.randomBoolean() ? null : ESTestCase.randomAlphaOfLengthBetween(1, 16)
                );
            }
            if (false == metadata.containsKey(AuthenticationField.API_KEY_CREATOR_REALM_NAME)) {
                assert false == metadata.containsKey(AuthenticationField.API_KEY_CREATOR_REALM_TYPE)
                    : "creator realm name and type must be both present or absent";
                final Authentication.RealmRef creatorRealmRef = randomRealmRef(
                    isRealmUnderDomain == null ? ESTestCase.randomBoolean() : isRealmUnderDomain
                );
                metadata.put(AuthenticationField.API_KEY_CREATOR_REALM_NAME, creatorRealmRef.getName());
                metadata.put(AuthenticationField.API_KEY_CREATOR_REALM_TYPE, creatorRealmRef.getType());
            }
            if (false == metadata.containsKey(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY)) {
                metadata.put(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY, new BytesArray("{}"));
            }
            if (false == metadata.containsKey(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY)) {
                metadata.put(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, new BytesArray("""
                    {"x":{"cluster":["all"],"indices":[{"names":["index*"],"privileges":["all"]}]}}"""));
            }
            if (false == metadata.containsKey(AuthenticationField.API_KEY_METADATA_KEY)) {
                if (ESTestCase.randomBoolean()) {
                    final Map<String, Object> keyMetadata = ESTestCase.randomFrom(
                        Map.of(
                            "application",
                            ESTestCase.randomAlphaOfLength(5),
                            "number",
                            1,
                            "numbers",
                            List.of(1, 3, 5),
                            "environment",
                            Map.of("os", "linux", "level", 42, "category", "trusted")
                        ),
                        Map.of(ESTestCase.randomAlphaOfLengthBetween(3, 8), ESTestCase.randomAlphaOfLengthBetween(3, 8)),
                        Map.of(),
                        null
                    );
                    if (keyMetadata != null) {
                        final BytesReference metadataBytes;
                        try {
                            metadataBytes = XContentTestUtils.convertToXContent(keyMetadata, XContentType.JSON);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        metadata.put(AuthenticationField.API_KEY_METADATA_KEY, metadataBytes);
                    }
                }
            }
        }

        private void prepareServiceAccountMetadata() {
            if (false == metadata.containsKey(ServiceAccountSettings.TOKEN_NAME_FIELD)) {
                metadata.put(ServiceAccountSettings.TOKEN_NAME_FIELD, ESTestCase.randomAlphaOfLength(8));
            }
            if (false == metadata.containsKey(ServiceAccountSettings.TOKEN_SOURCE_FIELD)) {
                metadata.put(
                    ServiceAccountSettings.TOKEN_SOURCE_FIELD,
                    ESTestCase.randomFrom(TokenInfo.TokenSource.values()).name().toLowerCase(Locale.ROOT)
                );
            }
        }

        private void resetShortcutRelatedVariables() {
            isServiceAccount = null;
            isRealmUnderDomain = null;
        }
    }
}
