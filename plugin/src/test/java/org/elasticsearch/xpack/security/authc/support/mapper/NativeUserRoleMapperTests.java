/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.mapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.security.authc.support.mapper.expressiondsl.FieldExpression.FieldPredicate;
import org.hamcrest.Matchers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NativeUserRoleMapperTests extends ESTestCase {

    public void testResolveRoles() throws Exception {
        // Does match DN
        final ExpressionRoleMapping mapping1 = new ExpressionRoleMapping("dept_h",
                new FieldExpression("dn", Collections.singletonList(FieldPredicate.create("*,ou=dept_h,o=forces,dc=gc,dc=ca"))),
                Arrays.asList("dept_h", "defence"), Collections.emptyMap(), true);
        // Does not match - user is not in this group
        final ExpressionRoleMapping mapping2 = new ExpressionRoleMapping("admin",
                new FieldExpression("groups",
                        Collections.singletonList(FieldPredicate.create("cn=esadmin,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca"))),
                Arrays.asList("admin"), Collections.emptyMap(), true);
        // Does match - user is one of these groups
        final ExpressionRoleMapping mapping3 = new ExpressionRoleMapping("flight",
                new FieldExpression("groups", Arrays.asList(
                        FieldPredicate.create("cn=alphaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca"),
                        FieldPredicate.create("cn=betaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca"),
                        FieldPredicate.create("cn=gammaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")
                )),
                Arrays.asList("flight"), Collections.emptyMap(), true);
        // Does not match - mapping is not enabled
        final ExpressionRoleMapping mapping4 = new ExpressionRoleMapping("mutants",
                new FieldExpression("groups",
                        Collections.singletonList(FieldPredicate.create("cn=mutants,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca"))),
                Arrays.asList("mutants"), Collections.emptyMap(), false);

        final InternalClient client = mock(InternalClient.class);
        final SecurityLifecycleService lifecycleService = mock(SecurityLifecycleService.class);
        when(lifecycleService.isSecurityIndexAvailable()).thenReturn(true);

        final NativeRoleMappingStore store = new NativeRoleMappingStore(Settings.EMPTY, client, lifecycleService) {
            @Override
            protected void loadMappings(ActionListener<List<ExpressionRoleMapping>> listener) {
                listener.onResponse(Arrays.asList(mapping1, mapping2, mapping3, mapping4));
            }
        };

        final RealmConfig realm = new RealmConfig("ldap1", Settings.EMPTY, Settings.EMPTY, mock(Environment.class),
                new ThreadContext(Settings.EMPTY));

        final PlainActionFuture<Set<String>> future = new PlainActionFuture<>();
        final UserRoleMapper.UserData user = new UserRoleMapper.UserData("sasquatch",
                "cn=walter.langowski,ou=people,ou=dept_h,o=forces,dc=gc,dc=ca",
                Arrays.asList(
                        "cn=alphaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca",
                        "cn=mutants,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca"
                ), Collections.emptyMap(), realm);

        store.resolveRoles(user, future);
        final Set<String> roles = future.get();
        assertThat(roles, Matchers.containsInAnyOrder("dept_h", "defence", "flight"));
    }

}