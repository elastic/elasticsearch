/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.Strings.collectionToDelimitedString;
import static org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings.AUTHZ_REALMS;

/**
 * Utility class for supporting "delegated authorization" (aka "authorization_realms", aka "lookup realms").
 * A {@link Realm} may support delegating authorization to another realm. It does this by registering a
 * setting for {@link DelegatedAuthorizationSettings#AUTHZ_REALMS}, and constructing an instance of this
 * class. Then, after the realm has performed any authentication steps, if {@link #hasDelegation()} is
 * {@code true}, it delegates the construction of the {@link User} object and {@link AuthenticationResult}
 * to {@link #resolve(String, ActionListener)}.
 */
public class DelegatedAuthorizationSupport {

    private final RealmUserLookup lookup;
    private final Logger logger;
    private final XPackLicenseState licenseState;

    /**
     * Resolves the {@link DelegatedAuthorizationSettings#AUTHZ_REALMS} setting from {@code config} and calls
     * {@link #DelegatedAuthorizationSupport(Iterable, List, Settings, ThreadContext, XPackLicenseState)}
     */
    public DelegatedAuthorizationSupport(Iterable<? extends Realm> allRealms, RealmConfig config, XPackLicenseState licenseState) {
        this(allRealms, config.getSetting(AUTHZ_REALMS), config.settings(), config.threadContext(),
            licenseState);
    }

    /**
     * Constructs a new object that delegates to the named realms ({@code lookupRealms}), which must exist within
     * {@code allRealms}.
     * @throws IllegalArgumentException if one of the specified realms does not exist
     */
    protected DelegatedAuthorizationSupport(Iterable<? extends Realm> allRealms, List<String> lookupRealms, Settings settings,
                                            ThreadContext threadContext, XPackLicenseState licenseState) {
        final List<Realm> resolvedLookupRealms = resolveRealms(allRealms, lookupRealms);
        checkForRealmChains(resolvedLookupRealms, settings);
        this.lookup = new RealmUserLookup(resolvedLookupRealms, threadContext);
        this.logger = LogManager.getLogger(getClass());
        this.licenseState = licenseState;
    }

    /**
     * Are there any realms configured for delegated lookup
     */
    public boolean hasDelegation() {
        return this.lookup.hasRealms();
    }

    /**
     * Attempts to find the user specified by {@code username} in one of the delegated realms.
     * The realms are searched in the order specified during construction.
     * Returns a {@link AuthenticationResult#success(User) successful result} if a {@link User}
     * was found, otherwise returns an
     * {@link AuthenticationResult#unsuccessful(String, Exception) unsuccessful result}
     * with a meaningful diagnostic message.
     */
    public void resolve(String username, ActionListener<AuthenticationResult> resultListener) {
        boolean authzOk = licenseState.isSecurityEnabled() && licenseState.isAllowed(Feature.SECURITY_AUTHORIZATION_REALM);
        if (authzOk == false) {
            resultListener.onResponse(AuthenticationResult.unsuccessful(
                DelegatedAuthorizationSettings.AUTHZ_REALMS_SUFFIX + " are not permitted",
                LicenseUtils.newComplianceException(DelegatedAuthorizationSettings.AUTHZ_REALMS_SUFFIX)
            ));
            return;
        }
        if (hasDelegation() == false) {
            resultListener.onResponse(AuthenticationResult.unsuccessful(
                "No [" + DelegatedAuthorizationSettings.AUTHZ_REALMS_SUFFIX + "] have been configured", null));
            return;
        }
        ActionListener<Tuple<User, Realm>> userListener = ActionListener.wrap(tuple -> {
            if (tuple != null) {
                logger.trace("Found user " + tuple.v1() + " in realm " + tuple.v2());
                resultListener.onResponse(AuthenticationResult.success(tuple.v1()));
            } else {
                resultListener.onResponse(AuthenticationResult.unsuccessful("the principal [" + username
                    + "] was authenticated, but no user could be found in realms [" + collectionToDelimitedString(lookup.getRealms(), ",")
                    + "]", null));
            }
        }, resultListener::onFailure);
        lookup.lookup(username, userListener);
    }

    private List<Realm> resolveRealms(Iterable<? extends Realm> allRealms, List<String> lookupRealms) {
        final List<Realm> result = new ArrayList<>(lookupRealms.size());
        for (String name : lookupRealms) {
            result.add(findRealm(name, allRealms));
        }
        assert result.size() == lookupRealms.size();
        return result;
    }

    /**
     * Checks for (and rejects) chains of delegation in the provided realms.
     * A chain occurs when "realmA" delegates authorization to "realmB", and realmB also delegates authorization (to any realm).
     * Since "realmB" does not handle its own authorization, it is not a valid target for delegated authorization.
     * @param delegatedRealms The list of realms that are going to be used for authorization. If is an error if any of these realms are
     *                        also configured to delegate their authorization.
     * @throws IllegalArgumentException if a chain is detected
     */
    private void checkForRealmChains(Iterable<Realm> delegatedRealms, Settings globalSettings) {
        for (Realm realm : delegatedRealms) {
            Setting<List<String>> realmAuthzSetting = AUTHZ_REALMS.apply(realm.type()).getConcreteSettingForNamespace(realm.name());
            if (realmAuthzSetting.exists(globalSettings)) {
                throw new IllegalArgumentException("cannot use realm [" + realm
                    + "] as an authorization realm - it is already delegating authorization to [" + realmAuthzSetting.get(globalSettings)
                    + "]");
            }
        }
    }

    private Realm findRealm(String name, Iterable<? extends Realm> allRealms) {
        for (Realm realm : allRealms) {
            if (name.equals(realm.name())) {
                return realm;
            }
        }
        throw new IllegalArgumentException("configured authorization realm [" + name + "] does not exist (or is not enabled)");
    }

}
