/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.core.security.user.APMSystemUser;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.BeatsSystemUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaSystemUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.LogstashSystemUser;
import org.elasticsearch.xpack.core.security.user.RemoteMonitoringUser;
import org.elasticsearch.xpack.core.security.user.ReservedUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore.ReservedUserInfo;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A realm for predefined users. These users can only be modified in terms of changing their passwords; no other modifications are allowed.
 * This realm is <em>always</em> enabled.
 */
public class ReservedRealm extends CachingUsernamePasswordRealm {

    public static final String TYPE = "reserved";
    public static final String NAME = "reserved";

    private final ReservedUserInfo bootstrapUserInfo;
    private final ReservedUserInfo autoconfigUserInfo;
    public static final Setting<SecureString> BOOTSTRAP_ELASTIC_PASSWORD = SecureSetting.secureString(
        "bootstrap.password",
        KeyStoreWrapper.SEED_SETTING
    );

    // we do not document this setting on the website because it mustn't be set by the users
    // it is only set by various installation scripts
    public static final Setting<SecureString> AUTOCONFIG_ELASTIC_PASSWORD_HASH = SecureSetting.secureString(
        "autoconfiguration.password_hash",
        null
    );

    private final NativeUsersStore nativeUsersStore;
    private final AnonymousUser anonymousUser;
    private final boolean realmEnabled;
    private final boolean anonymousEnabled;
    private final boolean elasticUserAutoconfigured;

    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());

    public ReservedRealm(
        Environment env,
        Settings settings,
        NativeUsersStore nativeUsersStore,
        AnonymousUser anonymousUser,
        ThreadPool threadPool
    ) {
        super(
            new RealmConfig(
                new RealmConfig.RealmIdentifier(TYPE, NAME),
                Settings.builder()
                    .put(settings)
                    .put(RealmSettings.realmSettingPrefix(new RealmConfig.RealmIdentifier(TYPE, NAME)) + "order", Integer.MIN_VALUE)
                    .build(),
                env,
                threadPool.getThreadContext()
            ),
            threadPool
        );
        this.nativeUsersStore = nativeUsersStore;
        this.realmEnabled = XPackSettings.RESERVED_REALM_ENABLED_SETTING.get(settings);
        this.anonymousUser = anonymousUser;
        this.anonymousEnabled = AnonymousUser.isAnonymousEnabled(settings);
        char[] autoconfigPasswordHash = null;
        // validate the password hash setting value, even if it is not going to be used
        if (AUTOCONFIG_ELASTIC_PASSWORD_HASH.exists(settings)) {
            autoconfigPasswordHash = AUTOCONFIG_ELASTIC_PASSWORD_HASH.get(settings).getChars();
            if (autoconfigPasswordHash.length == 0
                || Set.of(Hasher.SHA1, Hasher.MD5, Hasher.SSHA256, Hasher.NOOP).contains(Hasher.resolveFromHash(autoconfigPasswordHash))) {
                throw new IllegalArgumentException("Invalid password hash for elastic user auto configuration");
            }
        }
        elasticUserAutoconfigured = AUTOCONFIG_ELASTIC_PASSWORD_HASH.exists(settings)
            && false == BOOTSTRAP_ELASTIC_PASSWORD.exists(settings);
        if (elasticUserAutoconfigured) {
            autoconfigUserInfo = new ReservedUserInfo(autoconfigPasswordHash, true);
            bootstrapUserInfo = null;
        } else {
            autoconfigUserInfo = null;
            final Hasher reservedRealmHasher = Hasher.resolve(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings));
            final char[] hash = BOOTSTRAP_ELASTIC_PASSWORD.get(settings).length() == 0
                ? new char[0]
                : reservedRealmHasher.hash(BOOTSTRAP_ELASTIC_PASSWORD.get(settings));
            bootstrapUserInfo = new ReservedUserInfo(hash, true);
        }
    }

    @Override
    protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult<User>> listener) {
        if (realmEnabled == false) {
            listener.onResponse(AuthenticationResult.notHandled());
        } else if (ClientReservedRealm.isReserved(token.principal(), config.settings()) == false) {
            listener.onResponse(AuthenticationResult.notHandled());
        } else {
            getUserInfo(token.principal(), (userInfo) -> {
                if (userInfo != null) {
                    if (userInfo.hasEmptyPassword()) {
                        listener.onResponse(AuthenticationResult.terminate("failed to authenticate user [" + token.principal() + "]"));
                    } else {
                        ActionListener<AuthenticationResult<User>> hashCleanupListener = ActionListener.runBefore(listener, () -> {
                            if (userInfo != bootstrapUserInfo && userInfo != autoconfigUserInfo) {
                                Arrays.fill(userInfo.passwordHash, (char) 0);
                            }
                        });
                        if (userInfo.verifyPassword(token.credentials())) {
                            final User user = getUser(token.principal(), userInfo);
                            logDeprecatedUser(user);
                            // promote the auto-configured password as the elastic user password
                            if (userInfo == autoconfigUserInfo) {
                                assert ElasticUser.NAME.equals(token.principal());
                                nativeUsersStore.createElasticUser(userInfo.passwordHash, ActionListener.wrap(aVoid -> {
                                    hashCleanupListener.onResponse(AuthenticationResult.success(user));
                                }, e -> {
                                    // exceptionally, we must propagate a 500 or a 503 error if the auto config password hash
                                    // can't be promoted as the elastic user password, otherwise, such errors will
                                    // implicitly translate to 401s, which is wrong because the presented password was successfully
                                    // verified by the auto-config hash; the client must retry the request.
                                    listener.onFailure(
                                        Exceptions.authenticationProcessError(
                                            "failed to promote the auto-configured " + "elastic password hash",
                                            e
                                        )
                                    );
                                }));
                            } else {
                                hashCleanupListener.onResponse(AuthenticationResult.success(user));
                            }
                        } else {
                            hashCleanupListener.onResponse(
                                AuthenticationResult.terminate("failed to authenticate user [" + token.principal() + "]")
                            );
                        }
                    }
                } else {
                    listener.onResponse(AuthenticationResult.terminate("failed to authenticate user [" + token.principal() + "]"));
                }
            });
        }
    }

    @Override
    protected void doLookupUser(String username, ActionListener<User> listener) {
        if (realmEnabled == false) {
            if (anonymousEnabled && AnonymousUser.isAnonymousUsername(username, config.settings())) {
                listener.onResponse(anonymousUser);
            } else {
                listener.onResponse(null);
            }
        } else if (ClientReservedRealm.isReserved(username, config.settings()) == false) {
            listener.onResponse(null);
        } else if (AnonymousUser.isAnonymousUsername(username, config.settings())) {
            listener.onResponse(anonymousEnabled ? anonymousUser : null);
        } else {
            getUserInfo(username, (userInfo) -> {
                if (userInfo != null) {
                    listener.onResponse(getUser(username, userInfo));
                } else {
                    // this was a reserved username - don't allow this to go to another realm...
                    listener.onFailure(Exceptions.authenticationError("failed to lookup user [{}]", username));
                }
            });
        }
    }

    private ReservedUser getUser(String username, ReservedUserInfo userInfo) {
        assert username != null;
        switch (username) {
            case ElasticUser.NAME:
                return new ElasticUser(userInfo.enabled);
            case KibanaUser.NAME:
                return new KibanaUser(userInfo.enabled);
            case KibanaSystemUser.NAME:
                return new KibanaSystemUser(userInfo.enabled);
            case LogstashSystemUser.NAME:
                return new LogstashSystemUser(userInfo.enabled);
            case BeatsSystemUser.NAME:
                return new BeatsSystemUser(userInfo.enabled);
            case APMSystemUser.NAME:
                return new APMSystemUser(userInfo.enabled);
            case RemoteMonitoringUser.NAME:
                return new RemoteMonitoringUser(userInfo.enabled);
            default:
                if (anonymousEnabled && anonymousUser.principal().equals(username)) {
                    return anonymousUser;
                }
                return null;
        }
    }

    public void users(ActionListener<Collection<User>> listener) {
        if (realmEnabled == false) {
            listener.onResponse(anonymousEnabled ? Collections.singletonList(anonymousUser) : Collections.emptyList());
        } else {
            nativeUsersStore.getAllReservedUserInfo(ActionListener.wrap((reservedUserInfos) -> {
                final List<User> users = new ArrayList<>(8);

                ReservedUserInfo userInfo = reservedUserInfos.get(ElasticUser.NAME);
                users.add(new ElasticUser(userInfo == null || userInfo.enabled));

                userInfo = reservedUserInfos.get(KibanaUser.NAME);
                users.add(new KibanaUser(userInfo == null || userInfo.enabled));

                userInfo = reservedUserInfos.get(KibanaSystemUser.NAME);
                users.add(new KibanaSystemUser(userInfo == null || userInfo.enabled));

                userInfo = reservedUserInfos.get(LogstashSystemUser.NAME);
                users.add(new LogstashSystemUser(userInfo == null || userInfo.enabled));

                userInfo = reservedUserInfos.get(BeatsSystemUser.NAME);
                users.add(new BeatsSystemUser(userInfo == null || userInfo.enabled));

                userInfo = reservedUserInfos.get(APMSystemUser.NAME);
                users.add(new APMSystemUser(userInfo == null || userInfo.enabled));

                userInfo = reservedUserInfos.get(RemoteMonitoringUser.NAME);
                users.add(new RemoteMonitoringUser(userInfo == null || userInfo.enabled));

                if (anonymousEnabled) {
                    users.add(anonymousUser);
                }

                listener.onResponse(users);
            }, (e) -> {
                logger.error("failed to retrieve reserved users", e);
                listener.onResponse(anonymousEnabled ? Collections.singletonList(anonymousUser) : Collections.emptyList());
            }));
        }
    }

    private void getUserInfo(final String username, Consumer<ReservedUserInfo> consumer) {
        nativeUsersStore.getReservedUserInfo(username, ActionListener.wrap((userInfo) -> {
            if (userInfo == null) {
                consumer.accept(getDefaultUserInfo(username));
            } else {
                consumer.accept(userInfo);
            }
        }, (e) -> {
            logger.error((Supplier<?>) () -> "failed to retrieve password hash for reserved user [" + username + "]", e);
            consumer.accept(null);
        }));
    }

    private void logDeprecatedUser(final User user) {
        Map<String, Object> metadata = user.metadata();
        if (Boolean.TRUE.equals(metadata.get(MetadataUtils.DEPRECATED_METADATA_KEY))) {
            deprecationLogger.warn(
                DeprecationCategory.SECURITY,
                "deprecated_user-" + user.principal(),
                "The user ["
                    + user.principal()
                    + "] is deprecated and will be removed in a future version of Elasticsearch. "
                    + metadata.get(MetadataUtils.DEPRECATED_REASON_METADATA_KEY)
            );
        }
    }

    private ReservedUserInfo getDefaultUserInfo(String username) {
        if (ElasticUser.NAME.equals(username)) {
            if (elasticUserAutoconfigured) {
                assert bootstrapUserInfo == null;
                assert autoconfigUserInfo != null;
                return autoconfigUserInfo;
            } else {
                assert bootstrapUserInfo != null;
                assert autoconfigUserInfo == null;
                return bootstrapUserInfo;
            }
        } else {
            return ReservedUserInfo.defaultEnabledUserInfo();
        }
    }

    public static void addSettings(List<Setting<?>> settingsList) {
        settingsList.add(BOOTSTRAP_ELASTIC_PASSWORD);
        settingsList.add(AUTOCONFIG_ELASTIC_PASSWORD_HASH);
    }
}
