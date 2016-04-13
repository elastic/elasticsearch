/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.permission;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.shield.action.role.PutRoleAction;
import org.elasticsearch.shield.action.user.PutUserAction;
import org.elasticsearch.shield.authz.accesscontrol.IndicesAccessControl.IndexAccessControl;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;

import java.util.Map;

import static org.hamcrest.Matchers.is;

/**
 * Tests for the superuser role
 */
public class SuperuserRoleTests extends ESTestCase {

    public void testCluster() {
        final User user = new User("joe", SuperuserRole.NAME);
        final TransportRequest request = new TransportRequest.Empty();

        assertThat(SuperuserRole.INSTANCE.cluster().check(ClusterHealthAction.NAME, request, user), is(true));
        assertThat(SuperuserRole.INSTANCE.cluster().check(ClusterUpdateSettingsAction.NAME, request, user), is(true));
        assertThat(SuperuserRole.INSTANCE.cluster().check(PutUserAction.NAME, request, user), is(true));
        assertThat(SuperuserRole.INSTANCE.cluster().check(PutRoleAction.NAME, request, user), is(true));
        assertThat(SuperuserRole.INSTANCE.cluster().check(PutIndexTemplateAction.NAME, request, user), is(true));
        assertThat(SuperuserRole.INSTANCE.cluster().check("internal:admin/foo", request, user), is(false));
    }

    public void testIndices() {
        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final MetaData metaData = new MetaData.Builder()
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
                .build();

        Map<String, IndexAccessControl> authzMap =
                SuperuserRole.INSTANCE.indices().authorize(SearchAction.NAME, Sets.newHashSet("a1", "ba"), metaData);
        assertThat(authzMap.get("a1").isGranted(), is(true));
        assertThat(authzMap.get("b").isGranted(), is(true));
        authzMap = SuperuserRole.INSTANCE.indices().authorize(DeleteIndexAction.NAME, Sets.newHashSet("a1", "ba"), metaData);
        assertThat(authzMap.get("a1").isGranted(), is(true));
        assertThat(authzMap.get("b").isGranted(), is(true));
        authzMap = SuperuserRole.INSTANCE.indices().authorize(IndexAction.NAME, Sets.newHashSet("a2", "ba"), metaData);
        assertThat(authzMap.get("a2").isGranted(), is(true));
        assertThat(authzMap.get("b").isGranted(), is(true));
        authzMap = SuperuserRole.INSTANCE.indices().authorize(UpdateSettingsAction.NAME, Sets.newHashSet("aaaaaa", "ba"), metaData);
        assertThat(authzMap.get("aaaaaa").isGranted(), is(true));
        assertThat(authzMap.get("b").isGranted(), is(true));
    }

    public void testRunAs() {
        assertThat(SuperuserRole.INSTANCE.runAs().check(randomAsciiOfLengthBetween(1, 30)), is(true));
    }
}
