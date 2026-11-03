/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UserRoleMapperTests extends ESTestCase {
    public void testGroupPredicateSupportsManyGroupsWithoutOverflowingStack() {
        var groupCount = 10_000;
        var groups = IntStream.range(0, groupCount).mapToObj(i -> "cn=group-" + i + ",ou=groups,dc=example,dc=com").toList();

        // UserData only needs the realm name when constructing its expression model.
        var realm = mock(RealmConfig.class);
        when(realm.name()).thenReturn("ldap1");

        var user = new UserRoleMapper.UserData(
            "large-group-user",
            "uid=large-group-user,ou=users,dc=example,dc=com",
            groups,
            Map.of(),
            realm
        );

        var model = user.asModel();

        assertThat(model.test("groups", List.of(new FieldExpression.FieldValue("cn=group-1,ou=groups,dc=example,dc=com"))), is(true));

        // A missing value forces traversal of every group
        assertThat(model.test("groups", List.of(new FieldExpression.FieldValue("cn=missing,ou=groups,dc=example,dc=com"))), is(false));

        assertThat(model.test("groups", List.of(new FieldExpression.FieldValue("cn=group-9999,ou=groups,dc=example,dc=com"))), is(true));
    }
}
