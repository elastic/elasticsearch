/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.example.realm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class CustomRoleMappingRealmTests extends ESTestCase {

    public void testCachingOfUserLookup() throws Exception {
        final Environment env = super.newEnvironment();
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final RealmConfig realmConfig = new RealmConfig(
            new RealmConfig.RealmIdentifier(CustomRoleMappingRealm.TYPE, "test"),
            env.settings(), env, new ThreadContext(env.settings())
        );
        CustomRoleMappingRealm realm = new CustomRoleMappingRealm(realmConfig, roleMapper);

        final AtomicInteger roleMappingCounter = new AtomicInteger(0);
        mockRoleMapping(roleMapper, () -> {
            roleMappingCounter.incrementAndGet();
            return Set.of("role1", "role2");
        });

        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.lookupUser(CustomRoleMappingRealm.USERNAME, future);
        final User user1 = future.get();
        assertThat(user1.principal(), is(CustomRoleMappingRealm.USERNAME));
        assertThat(user1.roles(), arrayContainingInAnyOrder("role1", "role2"));
        assertThat(roleMappingCounter.get(), is(1));

        future = new PlainActionFuture<>();
        realm.lookupUser(CustomRoleMappingRealm.USERNAME, future);
        final User user2 = future.get();
        assertThat(user2, sameInstance(user1));
        assertThat(roleMappingCounter.get(), is(1));
    }

    @SuppressWarnings("unchecked")
    private void mockRoleMapping(UserRoleMapper roleMapper, Supplier<Set<String>> supplier) {
        doAnswer(inv -> {
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) inv.getArguments()[1];
            listener.onResponse(supplier.get());
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), any(ActionListener.class));
    }

}
