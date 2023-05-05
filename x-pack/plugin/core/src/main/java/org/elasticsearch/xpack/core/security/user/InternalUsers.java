/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class InternalUsers {

    private static final Logger logger = LogManager.getLogger(InternalUsers.class);

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

    public static InternalUser getUser(String username) {
        final var instance = INTERNAL_USERS.get(username);
        if (instance == null) {
            throw new IllegalStateException("user [" + username + "] is not internal");
        }
        return instance;
    }

    public static RoleDescriptor getRoleDescriptor(User user) {
        return findInternalUser(user).getLocalClusterRole().orElseThrow(() -> {
            throw new IllegalArgumentException("should never try to get the roles for internal user [" + user.principal() + "]");
        });
    }

    public static Map<String, RoleDescriptor> getRoleDescriptors() {
        return INTERNAL_USERS.values()
            .stream()
            .filter(instance -> instance.getLocalClusterRole().isPresent())
            .collect(Collectors.toMap(instance -> instance.principal(), instance -> instance.getLocalClusterRole().get()));
    }

    private static InternalUser findInternalUser(User user) {
        final var instance = INTERNAL_USERS.get(user.principal());
        if (instance != null) {
            if (instance == user) {
                return instance;
            }
            logger.debug(
                "User [" + user + "] has the same principal as internal user [" + instance + "] but is not the same user instance"
            );
        }
        throw new IllegalStateException("user [" + user + "] is not internal");
    }
}
