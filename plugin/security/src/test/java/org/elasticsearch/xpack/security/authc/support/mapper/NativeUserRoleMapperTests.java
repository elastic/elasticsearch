/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.mapper;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression.FieldValue;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NativeUserRoleMapperTests extends ESTestCase {

    public void testResolveRoles() throws Exception {
        // Does match DN
        final ExpressionRoleMapping mapping1 = new ExpressionRoleMapping("dept_h",
                new FieldExpression("dn", Collections.singletonList(new FieldValue("*,ou=dept_h,o=forces,dc=gc,dc=ca"))),
                Arrays.asList("dept_h", "defence"), Collections.emptyMap(), true);
        // Does not match - user is not in this group
        final ExpressionRoleMapping mapping2 = new ExpressionRoleMapping("admin",
                new FieldExpression("groups", Collections.singletonList(
                        new FieldValue(randomiseDn("cn=esadmin,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")))),
                Arrays.asList("admin"), Collections.emptyMap(), true);
        // Does match - user is one of these groups
        final ExpressionRoleMapping mapping3 = new ExpressionRoleMapping("flight",
                new FieldExpression("groups", Arrays.asList(
                        new FieldValue(randomiseDn("cn=alphaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")),
                        new FieldValue(randomiseDn("cn=betaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")),
                        new FieldValue(randomiseDn("cn=gammaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca"))
                )),
                Arrays.asList("flight"), Collections.emptyMap(), true);
        // Does not match - mapping is not enabled
        final ExpressionRoleMapping mapping4 = new ExpressionRoleMapping("mutants",
                new FieldExpression("groups", Collections.singletonList(
                        new FieldValue(randomiseDn("cn=mutants,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")))),
                Arrays.asList("mutants"), Collections.emptyMap(), false);

        final Client client = mock(Client.class);
        final SecurityLifecycleService lifecycleService = mock(SecurityLifecycleService.class);
        when(lifecycleService.isSecurityIndexAvailable()).thenReturn(true);

        final NativeRoleMappingStore store = new NativeRoleMappingStore(Settings.EMPTY, client, lifecycleService) {
            @Override
            protected void loadMappings(ActionListener<List<ExpressionRoleMapping>> listener) {
                final List<ExpressionRoleMapping> mappings = Arrays.asList(mapping1, mapping2, mapping3, mapping4);
                logger.info("Role mappings are: [{}]", mappings);
                listener.onResponse(mappings);
            }
        };

        final RealmConfig realm = new RealmConfig("ldap1", Settings.EMPTY, Settings.EMPTY, mock(Environment.class),
                new ThreadContext(Settings.EMPTY));

        final PlainActionFuture<Set<String>> future = new PlainActionFuture<>();
        final UserRoleMapper.UserData user = new UserRoleMapper.UserData("sasquatch",
                randomiseDn("cn=walter.langowski,ou=people,ou=dept_h,o=forces,dc=gc,dc=ca"),
                Arrays.asList(
                        randomiseDn("cn=alphaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca"),
                        randomiseDn("cn=mutants,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")
                ), Collections.emptyMap(), realm);

        logger.info("UserData is [{}]", user);
        store.resolveRoles(user, future);
        final Set<String> roles = future.get();
        assertThat(roles, Matchers.containsInAnyOrder("dept_h", "defence", "flight"));
    }

    private String randomiseDn(String dn) {
        // Randomly transform the dn into another valid form that is logically identical,
        // but (potentially) textually different
        switch (randomIntBetween(0, 3)) {
            case 0:
                // do nothing
                return dn;
            case 1:
                return dn.toUpperCase(Locale.ROOT);
            case 2:
                // Upper case just the attribute name for each RDN
                return Arrays.stream(dn.split(",")).map(s -> {
                    final String[] arr = s.split("=");
                    arr[0] = arr[0].toUpperCase(Locale.ROOT);
                    return String.join("=", arr);
                }).collect(Collectors.joining(","));
            case 3:
                return dn.replaceAll(",", ", ");
        }
        return dn;
    }

}
