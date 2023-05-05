/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InternalUsers {

    private static final Map<String, InternalUser> INTERNAL_USERS = new HashMap<>();

    static {
        defineUser(SystemUser.INSTANCE);
        defineUser(XPackUser.INSTANCE);
        defineUser(XPackSecurityUser.INSTANCE);
        defineUser(SecurityProfileUser.INSTANCE);
        defineUser(AsyncSearchUser.INSTANCE);
        defineUser(CrossClusterAccessUser.INSTANCE);
        defineUser(StorageInternalUser.INSTANCE);
    }

    private static void defineUser(InternalUser user) {
        INTERNAL_USERS.put(user.principal(), user);
    }

    public static Collection<InternalUser> get() {
        return Collections.unmodifiableCollection(INTERNAL_USERS.values());
    }

    public static InternalUser getUser(String username) {
        final var instance = INTERNAL_USERS.get(username);
        if (instance == null) {
            throw new IllegalStateException("user [" + username + "] is not internal");
        }
        return instance;
    }
}
