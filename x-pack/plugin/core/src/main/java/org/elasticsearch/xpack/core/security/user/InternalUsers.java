/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class InternalUsers {

    private record UserInstance(String name, User user, @Nullable RoleDescriptor role, Predicate<User> isUserPredicate) {
        private UserInstance {
            // Check that the parameters align
            assert User.isInternal(user) : "User " + user + " is not internal";
            // The user principal should match the official name
            assert name.equals(user.principal())
                : "User " + user + " has a principal [" + user.principal() + "] that dopes not match the provided name [" + name + "]";
            // The role descriptor should match the user name as well
            assert role == null || name.equals(role.getName())
                : "Internal user " + user + " should have a role named [" + name + "] but was [" + role.getName() + "]";
            // : The user object should match as an instance of the user.
            assert isUserPredicate.test(user) : "User " + user + " does not match provided predicate";
        }

        public boolean is(User u) {
            return isUserPredicate.test(u);
        }
    }

    private static final List<UserInstance> INTERNAL_USERS = List.of(
        new UserInstance(SystemUser.NAME, SystemUser.INSTANCE, null, SystemUser::is),
        new UserInstance(XPackUser.NAME, XPackUser.INSTANCE, XPackUser.ROLE_DESCRIPTOR, XPackUser::is),
        new UserInstance(XPackSecurityUser.NAME, XPackSecurityUser.INSTANCE, XPackSecurityUser.ROLE_DESCRIPTOR, XPackSecurityUser::is),
        new UserInstance(
            SecurityProfileUser.NAME,
            SecurityProfileUser.INSTANCE,
            SecurityProfileUser.ROLE_DESCRIPTOR,
            SecurityProfileUser::is
        ),
        new UserInstance(AsyncSearchUser.NAME, AsyncSearchUser.INSTANCE, AsyncSearchUser.ROLE_DESCRIPTOR, AsyncSearchUser::is),
        new UserInstance(CrossClusterAccessUser.NAME, CrossClusterAccessUser.INSTANCE, null, CrossClusterAccessUser::is)
    );

    private static UserInstance findInternalUser(User user) {
        return INTERNAL_USERS.stream()
            .filter(instance -> instance.is(user))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("user [" + user + "] is not internal"));
    }

    public static User getUser(String username) {
        return INTERNAL_USERS.stream()
            .filter(instance -> instance.name.equals(username))
            .map(UserInstance::user)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("user [" + username + "] is not internal"));
    }

    public static String getInternalUserName(User user) {
        assert User.isInternal(user);
        return findInternalUser(user).name;
    }

    public static RoleDescriptor getRoleDescriptor(User user) {
        assert User.isInternal(user);
        UserInstance instance = findInternalUser(user);
        if (instance.role == null) {
            throw new IllegalArgumentException(
                "the user ["
                    + user.principal()
                    + "] is the ["
                    + instance.name
                    + "] internal user and we should never try to get its role descriptors"
            );
        }
        return instance.role;
    }

    public static Map<String, RoleDescriptor> getRoleDescriptors() {
        return INTERNAL_USERS.stream()
            .filter(instance -> instance.role != null)
            .collect(Collectors.toMap(UserInstance::name, UserInstance::role));
    }

}
