/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.elasticsearch.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;
import org.junit.Before;

import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.CREATE;
import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.MONITOR;
import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.READ;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PermissionTests extends ESTestCase {
    private Role permission;

    @Before
    public void init() {
        Role.Builder builder = Role.builder("test");
        builder.add(MONITOR, "test_*", "/foo.*/");
        builder.add(READ, "baz_*foo", "/fool.*bar/");
        builder.add(MONITOR, "/bar.*/");
        builder.add(CREATE, "ingest_foo*");
        permission = builder.build();
    }

    public void testAllowedIndicesMatcherAction() throws Exception {
        testAllowedIndicesMatcher(permission.indices().allowedIndicesMatcher(GetAction.NAME));
    }

    public void testAllowedIndicesMatcherForMappingUpdates() throws Exception {
        for (String mappingUpdateActionName : List.of(PutMappingAction.NAME, AutoPutMappingAction.NAME)) {
            IndexAbstraction mockIndexAbstraction = mock(IndexAbstraction.class);
            Predicate<IndexAbstraction> indexPredicate = permission.indices().allowedIndicesMatcher(mappingUpdateActionName);
            // mapping updates are still permitted on indices and aliases
            when(mockIndexAbstraction.getName()).thenReturn("ingest_foo" + randomAlphaOfLength(3));
            when(mockIndexAbstraction.getType()).thenReturn(IndexAbstraction.Type.CONCRETE_INDEX);
            assertThat(indexPredicate.test(mockIndexAbstraction), is(true));
            when(mockIndexAbstraction.getType()).thenReturn(IndexAbstraction.Type.ALIAS);
            assertThat(indexPredicate.test(mockIndexAbstraction), is(true));
            // mapping updates are NOT permitted on data streams and backing indices
            when(mockIndexAbstraction.getType()).thenReturn(IndexAbstraction.Type.DATA_STREAM);
            assertThat(indexPredicate.test(mockIndexAbstraction), is(false));
            when(mockIndexAbstraction.getType()).thenReturn(IndexAbstraction.Type.CONCRETE_INDEX);
            when(mockIndexAbstraction.getParentDataStream()).thenReturn(mock(IndexAbstraction.DataStream.class));
            assertThat(indexPredicate.test(mockIndexAbstraction), is(false));
        }
    }

    public void testAllowedIndicesMatcherActionCaching() throws Exception {
        Predicate<IndexAbstraction> matcher1 = permission.indices().allowedIndicesMatcher(GetAction.NAME);
        Predicate<IndexAbstraction> matcher2 = permission.indices().allowedIndicesMatcher(GetAction.NAME);
        assertThat(matcher1, is(matcher2));
    }

    public void testBuildEmptyRole() {
        Role.Builder permission = Role.builder(new String[] { "some_role" });
        Role role = permission.build();
        assertThat(role, notNullValue());
        assertThat(role.cluster(), notNullValue());
        assertThat(role.indices(), notNullValue());
        assertThat(role.runAs(), notNullValue());
    }

    public void testRunAs() {
        Role permission = Role.builder("some_role")
                .runAs(new Privilege("name", "user1", "run*"))
                .build();
        assertThat(permission.runAs().check("user1"), is(true));
        assertThat(permission.runAs().check("user"), is(false));
        assertThat(permission.runAs().check("run" + randomAlphaOfLengthBetween(1, 10)), is(true));
    }

    // "baz_*foo", "/fool.*bar/"
    private void testAllowedIndicesMatcher(Predicate<IndexAbstraction> indicesMatcher) {
        assertThat(indicesMatcher.test(mockIndexAbstraction("foobar")), is(false));
        assertThat(indicesMatcher.test(mockIndexAbstraction("fool")), is(false));
        assertThat(indicesMatcher.test(mockIndexAbstraction("fool2bar")), is(true));
        assertThat(indicesMatcher.test(mockIndexAbstraction("baz_foo")), is(true));
        assertThat(indicesMatcher.test(mockIndexAbstraction("barbapapa")), is(false));
    }

    private IndexAbstraction mockIndexAbstraction(String name) {
        IndexAbstraction mock = mock(IndexAbstraction.class);
        when(mock.getName()).thenReturn(name);
        when(mock.getType()).thenReturn(randomFrom(IndexAbstraction.Type.CONCRETE_INDEX,
                IndexAbstraction.Type.ALIAS, IndexAbstraction.Type.DATA_STREAM));
        return mock;
    }
}
