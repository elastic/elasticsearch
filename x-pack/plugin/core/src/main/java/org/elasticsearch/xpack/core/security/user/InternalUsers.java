/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class InternalUsers {

    private record UserInstance(User user, @Nullable RoleDescriptor role, Predicate<User> isUserPredicate) {
        private UserInstance {
            // Check that the parameters align
            assert User.isInternal(user) : "User " + user + " is not internal";
            // The role descriptor should match the user name as well
            assert role == null || user.principal().equals(role.getName())
                : "Internal user " + user + " should have a role named [" + user.principal() + "] but was [" + role.getName() + "]";
            // : The user object should match as an instance of the user.
            assert isUserPredicate.test(user) : "User " + user + " does not match provided predicate";
        }

        public boolean is(User u) {
            return isUserPredicate.test(u);
        }
    }

    private static final Map<String, UserInstance> INTERNAL_USERS = new HashMap<>();
    static {
        defineUser(
            SystemUser.NAME,
            SystemUser.INSTANCE,
            null /* SystemUser relies on a simple action predicate rather than a role descriptor */,
            SystemUser::is
        );
        defineUser(XPackUser.NAME, XPackUser.INSTANCE, XPackUser.ROLE_DESCRIPTOR, XPackUser::is);
        defineUser(XPackSecurityUser.NAME, XPackSecurityUser.INSTANCE, XPackSecurityUser.ROLE_DESCRIPTOR, XPackSecurityUser::is);
        defineUser(SecurityProfileUser.NAME, SecurityProfileUser.INSTANCE, SecurityProfileUser.ROLE_DESCRIPTOR, SecurityProfileUser::is);
        defineUser(AsyncSearchUser.NAME, AsyncSearchUser.INSTANCE, AsyncSearchUser.ROLE_DESCRIPTOR, AsyncSearchUser::is);
        defineUser(
            CrossClusterAccessUser.NAME,
            CrossClusterAccessUser.INSTANCE,
            null /* CrossClusterAccessUser has a role descriptor, but it should never be resolved by this class */,
            CrossClusterAccessUser::is
        );
    }

    private static void defineUser(String name, User user, @Nullable RoleDescriptor roleDescriptor, Predicate<User> predicate) {
        assert name.equals(user.principal())
            : "User " + user + " has a principal [" + user.principal() + "] that does not match the provided name [" + name + "]";
        INTERNAL_USERS.put(name, new UserInstance(user, roleDescriptor, predicate));
    }

    private static UserInstance findInternalUser(User user) {
        final UserInstance instance = INTERNAL_USERS.get(user.principal());
        if (instance != null && instance.is(user)) {
            return instance;
        }
        throw new IllegalStateException("user [" + user + "] is not internal");
    }

    public static User getUser(String username) {
        final UserInstance instance = INTERNAL_USERS.get(username);
        if (instance == null) {
            throw new IllegalStateException("user [" + username + "] is not internal");
        }
        return instance.user;
    }

    public static String getInternalUserName(User user) {
        assert User.isInternal(user);
        return findInternalUser(user).user.principal();
    }

    public static RoleDescriptor getRoleDescriptor(User user) {
        assert User.isInternal(user);
        UserInstance instance = findInternalUser(user);
        if (instance.role == null) {
            throw new IllegalArgumentException("should never try to get the roles for internal user [" + user.principal() + "]");
        }
        return instance.role;
    }

    public static Map<String, RoleDescriptor> getRoleDescriptors() {
        return INTERNAL_USERS.values()
            .stream()
            .filter(instance -> instance.role != null)
            .collect(Collectors.toMap(instance -> instance.user().principal(), UserInstance::role));
    }

}
