/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.common.IteratingActionListener;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.Strings.collectionToDelimitedString;

public class DelegatedAuthorizationSupport {

    private final List<Realm> lookupRealms;
    private final ThreadContext threadContext;

    public DelegatedAuthorizationSupport(Iterable<? extends Realm> allRealms, RealmConfig config) {
        this(allRealms, DelegatedAuthorizationSettings.AUTHZ_REALMS.get(config.settings()), config.threadContext());
    }

    protected DelegatedAuthorizationSupport(Iterable<? extends Realm> allRealms, List<String> lookupRealms, ThreadContext threadContext) {
        this.lookupRealms = resolveRealms(allRealms, lookupRealms);
        this.threadContext = threadContext;
    }

    public boolean hasDelegation() {
        return this.lookupRealms.isEmpty() == false;
    }

    public void resolveUser(String username, ActionListener<AuthenticationResult> resultListener) {
        if (lookupRealms.isEmpty()) {
            throw new IllegalStateException("No realms have been configured for delegation");
        }
        ActionListener<User> userListener = ActionListener.wrap(user -> {
            if (user != null) {
                resultListener.onResponse(AuthenticationResult.success(user));
            } else {
                resultListener.onResponse(AuthenticationResult.unsuccessful("the principal [" + username
                    + "] was authenticated, but no user could be found in realms [" + collectionToDelimitedString(lookupRealms, ",")
                    + "]", null));
            }
        }, resultListener::onFailure);
        final IteratingActionListener<User, Realm> iteratingListener = new IteratingActionListener<>(userListener,
            (realm, listener) -> realm.lookupUser(username, listener),
            lookupRealms, threadContext);
        iteratingListener.run();
    }

    private List<Realm> resolveRealms(Iterable<? extends Realm> allRealms, List<String> lookupRealms) {
        final List<Realm> result = new ArrayList<>(lookupRealms.size());
        for (String name : lookupRealms) {
            result.add(findRealm(name, allRealms));
        }
        assert result.size() == lookupRealms.size();
        return result;
    }

    private Realm findRealm(String name, Iterable<? extends Realm> allRealms) {
        for (Realm realm : allRealms) {
            if (name.equals(realm.name())) {
                return realm;
            }
        }
        throw new IllegalStateException("configured authorizing realm [" + name + "] does not exist");
    }

}
