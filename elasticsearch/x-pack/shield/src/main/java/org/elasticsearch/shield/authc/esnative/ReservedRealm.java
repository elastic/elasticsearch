/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esnative;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.esnative.NativeUsersStore.ChangeListener;
import org.elasticsearch.shield.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.support.Exceptions;
import org.elasticsearch.shield.user.AnonymousUser;
import org.elasticsearch.shield.user.KibanaUser;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.user.XPackUser;

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
            case XPackUser.NAME:
            case KibanaUser.NAME:
                return true;
            default:
                return AnonymousUser.isAnonymousUsername(username);
        }
    }

    public static User getUser(String username) {
        assert username != null;
        switch (username) {
            case XPackUser.NAME:
                return XPackUser.INSTANCE;
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
            return Arrays.asList(XPackUser.INSTANCE, KibanaUser.INSTANCE, AnonymousUser.INSTANCE);
        }
        return Arrays.asList(XPackUser.INSTANCE, KibanaUser.INSTANCE);
    }

    private char[] getPasswordHash(final String username) {
        if (nativeUsersStore.started() == false) {
            // we need to be able to check for the user store being started...
            return null;
        }

        if (nativeUsersStore.shieldIndexExists() == false) {
            return DEFAULT_PASSWORD_HASH;
        }
        try {
            char[] passwordHash = nativeUsersStore.reservedUserPassword(username);
            if (passwordHash == null) {
                return DEFAULT_PASSWORD_HASH;
            }
            return passwordHash;
        } catch (Throwable e) {
            logger.error("failed to retrieve password hash for reserved user [{}]", e, username);
            return null;
        }
    }
}
