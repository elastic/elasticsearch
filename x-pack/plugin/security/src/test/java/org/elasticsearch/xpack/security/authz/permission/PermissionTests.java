/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission.IsResourceAuthorizedPredicate;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.CREATE;
import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.MONITOR;
import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.READ;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PermissionTests extends ESTestCase {
    private static final RestrictedIndices EMPTY_RESTRICTED_INDICES = new RestrictedIndices(Automatons.EMPTY);
    private Role permission;

    @Before
    public void init() {
        Role.Builder builder = Role.builder(EMPTY_RESTRICTED_INDICES, "test");
        builder.add(MONITOR, "test_*", "/foo.*/");
        builder.add(READ, "baz_*foo", "/fool.*bar/");
        builder.add(MONITOR, "/bar.*/");
        builder.add(CREATE, "ingest_foo*");
        permission = builder.build();
    }

    public void testAllowedIndicesMatcherAction() throws Exception {
        testAllowedIndicesMatcher(permission.indices().allowedIndicesMatcher(TransportGetAction.TYPE.name()));
    }

    public void testAllowedIndicesMatcherForMappingUpdates() throws Exception {
        for (String mappingUpdateActionName : List.of(TransportPutMappingAction.TYPE.name(), TransportAutoPutMappingAction.TYPE.name())) {
            IndexAbstraction mockIndexAbstraction = mock(IndexAbstraction.class);
            IsResourceAuthorizedPredicate indexPredicate = permission.indices().allowedIndicesMatcher(mappingUpdateActionName);
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
            when(mockIndexAbstraction.getParentDataStream()).thenReturn(
                DataStreamTestHelper.newInstance("ds", List.of(new Index("idx", UUIDs.randomBase64UUID(random()))))
            );
            assertThat(indexPredicate.test(mockIndexAbstraction), is(false));
        }
    }

    public void testAllowedIndicesMatcherActionCaching() throws Exception {
        IsResourceAuthorizedPredicate matcher1 = permission.indices().allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IsResourceAuthorizedPredicate matcher2 = permission.indices().allowedIndicesMatcher(TransportGetAction.TYPE.name());
        assertThat(matcher1, is(matcher2));
    }

    public void testBuildEmptyRole() {
        Role.Builder permission = Role.builder(EMPTY_RESTRICTED_INDICES, "some_role");
        Role role = permission.build();
        assertThat(role, notNullValue());
        assertThat(role.cluster(), notNullValue());
        assertThat(role.indices(), notNullValue());
        assertThat(role.runAs(), notNullValue());
    }

    public void testRunAs() {
        Role permission = Role.builder(EMPTY_RESTRICTED_INDICES, "some_role").runAs(new Privilege("name", "user1", "run*")).build();
        assertThat(permission.runAs().check("user1"), is(true));
        assertThat(permission.runAs().check("user"), is(false));
        assertThat(permission.runAs().check("run" + randomAlphaOfLengthBetween(1, 10)), is(true));
    }

    // "baz_*foo", "/fool.*bar/"
    private void testAllowedIndicesMatcher(IsResourceAuthorizedPredicate indicesMatcher) {
        assertThat(indicesMatcher.test(mockIndexAbstraction("foobar")), is(false));
        assertThat(indicesMatcher.test(mockIndexAbstraction("fool")), is(false));
        assertThat(indicesMatcher.test(mockIndexAbstraction("fool2bar")), is(true));
        assertThat(indicesMatcher.test(mockIndexAbstraction("baz_foo")), is(true));
        assertThat(indicesMatcher.test(mockIndexAbstraction("barbapapa")), is(false));
    }

    private IndexAbstraction mockIndexAbstraction(String name) {
        IndexAbstraction mock = mock(IndexAbstraction.class);
        when(mock.getName()).thenReturn(name);
        when(mock.getType()).thenReturn(
            randomFrom(IndexAbstraction.Type.CONCRETE_INDEX, IndexAbstraction.Type.ALIAS, IndexAbstraction.Type.DATA_STREAM)
        );
        return mock;
    }
}
