/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.common.IteratingActionListener;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Collections;
import java.util.List;

public class RealmUserLookup {

    private final List<? extends Realm> realms;
    private final ThreadContext threadContext;

    public RealmUserLookup(List<? extends Realm> realms, ThreadContext threadContext) {
        this.realms = realms;
        this.threadContext = threadContext;
    }

    public List<Realm> getRealms() {
        return Collections.unmodifiableList(realms);
    }

    public boolean hasRealms() {
        return realms.isEmpty() == false;
    }

    /**
     * Lookup the {@code principal} in the list of {@link #realms}.
     * The realms are consulted in order. When one realm responds with a non-null {@link User}, this
     * is returned with the matching realm, through the {@code listener}.
     * If no user if found (including the case where the {@link #realms} list is empty), then
     * {@link ActionListener#onResponse(Object)} is called with a {@code null} {@link Tuple}.
     */
    public void lookup(String principal, ActionListener<Tuple<User, Realm>> listener) {
        final IteratingActionListener<Tuple<User, Realm>, ? extends Realm> userLookupListener =
            new IteratingActionListener<>(listener,
                (realm, lookupUserListener) -> realm.lookupUser(principal,
                    ActionListener.wrap(foundUser -> {
                            if (foundUser != null) {
                                lookupUserListener.onResponse(new Tuple<>(foundUser, realm));
                            } else {
                                lookupUserListener.onResponse(null);
                            }
                        },
                        lookupUserListener::onFailure)),
                realms, threadContext);
        try {
            userLookupListener.run();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
