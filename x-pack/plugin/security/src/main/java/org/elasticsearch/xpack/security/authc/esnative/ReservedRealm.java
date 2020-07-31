/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore.ReservedUserInfo;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A realm for predefined users. These users can only be modified in terms of changing their passwords; no other modifications are allowed.
 * This realm is <em>always</em> enabled.
 */
public class ReservedRealm extends CachingUsernamePasswordRealm {

    public static final String TYPE = "reserved";

    private final ReservedUserInfo bootstrapUserInfo;
    public static final Setting<SecureString> BOOTSTRAP_ELASTIC_PASSWORD = SecureSetting.secureString("bootstrap.password",
            KeyStoreWrapper.SEED_SETTING);

    private final NativeUsersStore nativeUsersStore;
    private final AnonymousUser anonymousUser;
    private final boolean realmEnabled;
    private final boolean anonymousEnabled;
    private final SecurityIndexManager securityIndex;
    private final Hasher reservedRealmHasher;
    private final ReservedUserInfo disabledDefaultUserInfo;
    private final ReservedUserInfo enabledDefaultUserInfo;

    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());

    public ReservedRealm(Environment env, Settings settings, NativeUsersStore nativeUsersStore, AnonymousUser anonymousUser,
                         SecurityIndexManager securityIndex, ThreadPool threadPool) {
        super(new RealmConfig(new RealmConfig.RealmIdentifier(TYPE, TYPE),
            Settings.builder()
                .put(settings)
                .put(RealmSettings.realmSettingPrefix(new RealmConfig.RealmIdentifier(TYPE, TYPE)) + "order", Integer.MIN_VALUE)
                .build(), env, threadPool.getThreadContext()), threadPool);
        this.nativeUsersStore = nativeUsersStore;
        this.realmEnabled = XPackSettings.RESERVED_REALM_ENABLED_SETTING.get(settings);
        this.anonymousUser = anonymousUser;
        this.anonymousEnabled = AnonymousUser.isAnonymousEnabled(settings);
        this.securityIndex = securityIndex;
        this.reservedRealmHasher = Hasher.resolve(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings));
        final char[] emptyPasswordHash = reservedRealmHasher.hash(new SecureString("".toCharArray()));
        disabledDefaultUserInfo = new ReservedUserInfo(emptyPasswordHash, false, true);
        enabledDefaultUserInfo = new ReservedUserInfo(emptyPasswordHash, true, true);
        final char[] hash = BOOTSTRAP_ELASTIC_PASSWORD.get(settings).length() == 0 ? emptyPasswordHash :
            reservedRealmHasher.hash(BOOTSTRAP_ELASTIC_PASSWORD.get(settings));
        bootstrapUserInfo = new ReservedUserInfo(hash, true, hash == emptyPasswordHash);
    }

    @Override
    protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult> listener) {
        if (realmEnabled == false) {
            listener.onResponse(AuthenticationResult.notHandled());
        } else if (ClientReservedRealm.isReserved(token.principal(), config.settings()) == false) {
            listener.onResponse(AuthenticationResult.notHandled());
        } else {
            getUserInfo(token.principal(), ActionListener.wrap((userInfo) -> {
                AuthenticationResult result;
                if (userInfo != null) {
                    try {
                        if (userInfo.hasEmptyPassword) {
                            result = AuthenticationResult.terminate("failed to authenticate user [" + token.principal() + "]", null);
                        } else if (userInfo.verifyPassword(token.credentials())) {
                            final User user = getUser(token.principal(), userInfo);
                            logDeprecatedUser(user);
                            result = AuthenticationResult.success(user);
                        } else {
                            result = AuthenticationResult.terminate("failed to authenticate user [" + token.principal() + "]", null);
                        }
                    } finally {
                        assert userInfo.passwordHash != disabledDefaultUserInfo.passwordHash : "default user info must be cloned";
                        assert userInfo.passwordHash != enabledDefaultUserInfo.passwordHash : "default user info must be cloned";
                        assert userInfo.passwordHash != bootstrapUserInfo.passwordHash : "bootstrap user info must be cloned";
                        Arrays.fill(userInfo.passwordHash, (char) 0);
                    }
                } else {
                    result = AuthenticationResult.terminate("failed to authenticate user [" + token.principal() + "]", null);
                }
                // we want the finally block to clear out the chars before we proceed further so we handle the result here
                listener.onResponse(result);
            }, listener::onFailure));
        }
    }

    @Override
    protected void doLookupUser(String username, ActionListener<User> listener) {
        if (realmEnabled == false) {
            if (anonymousEnabled && AnonymousUser.isAnonymousUsername(username, config.settings())) {
                listener.onResponse(anonymousUser);
            }
            listener.onResponse(null);
        } else if (ClientReservedRealm.isReserved(username, config.settings()) == false) {
            listener.onResponse(null);
        } else if (AnonymousUser.isAnonymousUsername(username, config.settings())) {
            listener.onResponse(anonymousEnabled ? anonymousUser : null);
        } else {
            getUserInfo(username, ActionListener.wrap((userInfo) -> {
                if (userInfo != null) {
                    listener.onResponse(getUser(username, userInfo));
                } else {
                    // this was a reserved username - don't allow this to go to another realm...
                    listener.onFailure(Exceptions.authenticationError("failed to lookup user [{}]", username));
                }
            }, listener::onFailure));
        }
    }

    private User getUser(String username, ReservedUserInfo userInfo) {
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
                List<User> users = new ArrayList<>(4);

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


    private void getUserInfo(final String username, ActionListener<ReservedUserInfo> listener) {
        if (securityIndex.indexExists() == false) {
            listener.onResponse(getDefaultUserInfo(username));
        } else {
            nativeUsersStore.getReservedUserInfo(username, ActionListener.wrap((userInfo) -> {
                if (userInfo == null) {
                    listener.onResponse(getDefaultUserInfo(username));
                } else {
                    listener.onResponse(userInfo);
                }
            }, (e) -> {
                logger.error((Supplier<?>) () ->
                        new ParameterizedMessage("failed to retrieve password hash for reserved user [{}]", username), e);
                listener.onResponse(null);
            }));
        }
    }

    private void logDeprecatedUser(final User user){
        Map<String, Object> metadata = user.metadata();
        if (Boolean.TRUE.equals(metadata.get(MetadataUtils.DEPRECATED_METADATA_KEY))) {
            deprecationLogger.deprecate("deprecated_user-" + user.principal(), "The user [" + user.principal() +
                    "] is deprecated and will be removed in a future version of Elasticsearch. " +
                    metadata.get(MetadataUtils.DEPRECATED_REASON_METADATA_KEY));
        }
    }

    private ReservedUserInfo getDefaultUserInfo(String username) {
        if (ElasticUser.NAME.equals(username)) {
            return bootstrapUserInfo.deepClone();
        } else {
            return enabledDefaultUserInfo.deepClone();
        }
    }

    public static void addSettings(List<Setting<?>> settingsList) {
        settingsList.add(BOOTSTRAP_ELASTIC_PASSWORD);
    }
}
