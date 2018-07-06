/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.Strings.collectionToDelimitedString;

public class DelegatedAuthorizationSupport {

    private final RealmUserLookup lookup;
    private final Logger logger;

    public DelegatedAuthorizationSupport(Iterable<? extends Realm> allRealms, RealmConfig config) {
        this(allRealms, DelegatedAuthorizationSettings.AUTHZ_REALMS.get(config.settings()), config.threadContext());
    }

    protected DelegatedAuthorizationSupport(Iterable<? extends Realm> allRealms, List<String> lookupRealms, ThreadContext threadContext) {
       this.lookup = new RealmUserLookup(resolveRealms(allRealms, lookupRealms), threadContext);
       this.logger = Loggers.getLogger(getClass());
    }

    public boolean hasDelegation() {
        return this.lookup.hasRealms();
    }

    public void resolve(String username, ActionListener<AuthenticationResult> resultListener) {
        if (hasDelegation() == false) {
            resultListener.onResponse(AuthenticationResult.unsuccessful("No realms have been configured for delegation", null));
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

    private Realm findRealm(String name, Iterable<? extends Realm> allRealms) {
        for (Realm realm : allRealms) {
            if (name.equals(realm.name())) {
                return realm;
            }
        }
        throw new IllegalStateException("configured authorizing realm [" + name + "] does not exist (or is not enabled)");
    }

}
