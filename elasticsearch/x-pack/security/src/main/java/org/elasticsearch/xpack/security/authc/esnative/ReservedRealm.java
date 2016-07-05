/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore.ChangeListener;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.support.Exceptions;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.user.KibanaUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.user.ElasticUser;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * A realm for predefined users. These users can only be modified in terms of changing their passwords; no other modifications are allowed.
 * This realm is <em>always</em> enabled.
 */
public class ReservedRealm extends CachingUsernamePasswordRealm {

    public static final String TYPE = "reserved";
    private static final char[] DEFAULT_PASSWORD_HASH = Hasher.BCRYPT.hash(new SecuredString("changeme".toCharArray()));

    private final NativeUsersStore nativeUsersStore;

    @Inject
    public ReservedRealm(Environment env, Settings settings, NativeUsersStore nativeUsersStore) {
        super(TYPE, new RealmConfig(TYPE, Settings.EMPTY, settings, env));
        this.nativeUsersStore = nativeUsersStore;
        nativeUsersStore.addListener(new ChangeListener() {
            @Override
            public void onUsersChanged(List<String> changedUsers) {
                changedUsers.stream()
                        .filter(ReservedRealm::isReserved)
                        .forEach(ReservedRealm.this::expire);
            }
        });

    }

    @Override
    protected User doAuthenticate(UsernamePasswordToken token) {
        final User user = getUser(token.principal());
        if (user == null) {
            return null;
        }

        final char[] passwordHash = getPasswordHash(user.principal());
        if (passwordHash != null) {
            try {
                if (Hasher.BCRYPT.verify(token.credentials(), passwordHash)) {
                    return user;
                }
            } finally {
                if (passwordHash != DEFAULT_PASSWORD_HASH) {
                    Arrays.fill(passwordHash, (char) 0);
                }
            }
        }
        // this was a reserved username - don't allow this to go to another realm...
        throw Exceptions.authenticationError("failed to authenticate user [{}]", token.principal());
    }

    @Override
    protected User doLookupUser(String username) {
        return getUser(username);
    }

    @Override
    public boolean userLookupSupported() {
        return true;
    }

    public static boolean isReserved(String username) {
        assert username != null;
        switch (username) {
            case ElasticUser.NAME:
            case KibanaUser.NAME:
                return true;
            default:
                return AnonymousUser.isAnonymousUsername(username);
        }
    }

    public static User getUser(String username) {
        assert username != null;
        switch (username) {
            case ElasticUser.NAME:
                return ElasticUser.INSTANCE;
            case KibanaUser.NAME:
                return KibanaUser.INSTANCE;
            default:
                if (AnonymousUser.enabled() && AnonymousUser.isAnonymousUsername(username)) {
                    return AnonymousUser.INSTANCE;
                }
                return null;
        }
    }

    public static Collection<User> users() {
        if (AnonymousUser.enabled()) {
            return Arrays.asList(ElasticUser.INSTANCE, KibanaUser.INSTANCE, AnonymousUser.INSTANCE);
        }
        return Arrays.asList(ElasticUser.INSTANCE, KibanaUser.INSTANCE);
    }

    private char[] getPasswordHash(final String username) {
        if (nativeUsersStore.started() == false) {
            // we need to be able to check for the user store being started...
            return null;
        }

        if (nativeUsersStore.securityIndexExists() == false) {
            return DEFAULT_PASSWORD_HASH;
        }
        try {
            char[] passwordHash = nativeUsersStore.reservedUserPassword(username);
            if (passwordHash == null) {
                return DEFAULT_PASSWORD_HASH;
            }
            return passwordHash;
        } catch (Exception e) {
            logger.error("failed to retrieve password hash for reserved user [{}]", e, username);
            return null;
        }
    }
}
