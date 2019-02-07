/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.contains;

public class AuthorizedIndicesTests extends ESTestCase {

    public void testAuthorizedIndicesUserWithoutRoles() {
        AuthorizedIndices authorizedIndices = new AuthorizedIndices(Role.EMPTY, "", MetaData.EMPTY_META_DATA);
        List<String> list = authorizedIndices.get();
        assertTrue(list.isEmpty());
    }

    public void testAuthorizedIndicesUserWithSomeRoles() {
        RoleDescriptor aStarRole = new RoleDescriptor("a_star", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a*").privileges("all").build() }, null);
        RoleDescriptor bRole = new RoleDescriptor("b", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("b").privileges("READ").build() }, null);
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        MetaData metaData = MetaData.builder()
                .put(new IndexMetaData.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("aaaaaa").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("bbbbb").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("b")
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(new AliasMetaData.Builder("ab").build())
                        .putAlias(new AliasMetaData.Builder("ba").build())
                        .build(), true)
                .put(new IndexMetaData.Builder(RestrictedIndicesNames.INTERNAL_SECURITY_INDEX)
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(new AliasMetaData.Builder(RestrictedIndicesNames.SECURITY_INDEX_NAME).build())
                        .build(), true)
                .build();
        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        final Set<RoleDescriptor> descriptors = Sets.newHashSet(aStarRole, bRole);
        CompositeRolesStore.buildRoleFromDescriptors(descriptors, new FieldPermissionsCache(Settings.EMPTY), null, future);
        Role roles = future.actionGet();
        AuthorizedIndices authorizedIndices = new AuthorizedIndices(roles, SearchAction.NAME, metaData);
        List<String> list = authorizedIndices.get();
        assertThat(list, containsInAnyOrder("a1", "a2", "aaaaaa", "b", "ab"));
        assertFalse(list.contains("bbbbb"));
        assertFalse(list.contains("ba"));
        assertThat(list, not(contains(RestrictedIndicesNames.INTERNAL_SECURITY_INDEX)));
        assertThat(list, not(contains(RestrictedIndicesNames.SECURITY_INDEX_NAME)));
    }

    public void testAuthorizedIndicesUserWithSomeRolesEmptyMetaData() {
        Role role = Role.builder("role").add(IndexPrivilege.ALL, "*").build();
        AuthorizedIndices authorizedIndices = new AuthorizedIndices(role, SearchAction.NAME, MetaData.EMPTY_META_DATA);
        List<String> list = authorizedIndices.get();
        assertTrue(list.isEmpty());
    }

    public void testSecurityIndicesAreRestrictedForDefaultRole() {
        Role role = Role.builder(randomFrom("user_role", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()))
                .add(IndexPrivilege.ALL, "*")
                .cluster(ClusterPrivilege.ALL)
                .build();
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        MetaData metaData = MetaData.builder()
                .put(new IndexMetaData.Builder("an-index").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("another-index").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder(RestrictedIndicesNames.INTERNAL_SECURITY_INDEX)
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(new AliasMetaData.Builder(RestrictedIndicesNames.SECURITY_INDEX_NAME).build())
                        .build(), true)
                .build();

        AuthorizedIndices authorizedIndices = new AuthorizedIndices(role, SearchAction.NAME, metaData);
        List<String> list = authorizedIndices.get();
        assertThat(list, containsInAnyOrder("an-index", "another-index"));
        assertThat(list, not(contains(RestrictedIndicesNames.INTERNAL_SECURITY_INDEX)));
        assertThat(list, not(contains(RestrictedIndicesNames.SECURITY_INDEX_NAME)));
    }

    public void testSecurityIndicesAreNotRemovedFromUnrestrictedRole() {
        Role role = Role.builder(randomAlphaOfLength(8))
                .add(FieldPermissions.DEFAULT, null, IndexPrivilege.ALL, true, "*")
                .cluster(ClusterPrivilege.ALL)
                .build();
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        MetaData metaData = MetaData.builder()
                .put(new IndexMetaData.Builder("an-index").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("another-index").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder(RestrictedIndicesNames.INTERNAL_SECURITY_INDEX)
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(new AliasMetaData.Builder(RestrictedIndicesNames.SECURITY_INDEX_NAME).build())
                        .build(), true)
                .build();

        AuthorizedIndices authorizedIndices = new AuthorizedIndices(role, SearchAction.NAME, metaData);
        List<String> list = authorizedIndices.get();
        assertThat(list, containsInAnyOrder("an-index", "another-index", SecurityIndexManager.SECURITY_INDEX_NAME,
                SecurityIndexManager.INTERNAL_SECURITY_INDEX));

        AuthorizedIndices authorizedIndicesSuperUser = new AuthorizedIndices(ReservedRolesStore.SUPERUSER_ROLE, SearchAction.NAME,
                metaData);
        assertThat(authorizedIndicesSuperUser.get(), containsInAnyOrder("an-index", "another-index",
                SecurityIndexManager.SECURITY_INDEX_NAME, SecurityIndexManager.INTERNAL_SECURITY_INDEX));
    }
}
