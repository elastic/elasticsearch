/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public final class DeprecationRoleDescriptorConsumerTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        this.threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        Mockito.doAnswer((Answer) invocation -> {
            final Runnable arg0 = (Runnable) invocation.getArguments()[0];
            arg0.run();
            return null;
        }).when(executorService).execute(Mockito.isA(Runnable.class));
        when(threadPool.generic()).thenReturn(executorService);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    }

    public void testSimpleAliasAndIndexPair() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        addIndex(metaDataBuilder, "index", "alias");
        final RoleDescriptor roleOverAlias = new RoleDescriptor("roleOverAlias", new String[] { "read" },
                new RoleDescriptor.IndicesPrivileges[] { indexPrivileges(randomFrom("read", "write", "delete", "index"), "alias") }, null);
        final RoleDescriptor roleOverIndex = new RoleDescriptor("roleOverIndex", new String[] { "manage" },
                new RoleDescriptor.IndicesPrivileges[] { indexPrivileges(randomFrom("read", "write", "delete", "index"), "index") }, null);
        DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
                mockClusterService(metaDataBuilder.build()), threadPool, deprecationLogger);
        deprecationConsumer.accept(Arrays.asList(roleOverAlias, roleOverIndex));
        verifyLogger(deprecationLogger, "roleOverAlias", "alias", "index");
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testRoleGrantsOnIndexAndAliasPair() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        addIndex(metaDataBuilder, "index", "alias");
        addIndex(metaDataBuilder, "index1", "alias2");
        final RoleDescriptor roleOverIndexAndAlias = new RoleDescriptor("roleOverIndexAndAlias", new String[] { "manage_watcher" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges(randomFrom("read", "write", "delete", "index"), "index", "alias") },
                null);
        DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
                mockClusterService(metaDataBuilder.build()), threadPool, deprecationLogger);
        deprecationConsumer.accept(Arrays.asList(roleOverIndexAndAlias));
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testMultiplePrivilegesLoggedOnce() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        addIndex(metaDataBuilder, "index", "alias");
        addIndex(metaDataBuilder, "index2", "alias2");
        final RoleDescriptor roleOverAlias = new RoleDescriptor("roleOverAlias", new String[] { "manage_watcher" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges("write", "alias"),
                        indexPrivileges("manage_ilm", "alias") },
                null);
        DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
                mockClusterService(metaDataBuilder.build()), threadPool, deprecationLogger);
        deprecationConsumer.accept(Arrays.asList(roleOverAlias));
        verifyLogger(deprecationLogger, "roleOverAlias", "alias", "index");
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testMultiplePrivilegesLoggedForEachAlias() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        addIndex(metaDataBuilder, "index", "alias", "alias3");
        addIndex(metaDataBuilder, "index2", "alias2", "alias", "alias4");
        addIndex(metaDataBuilder, "index3", "alias3", "alias");
        addIndex(metaDataBuilder, "index4", "alias4", "alias");
        addIndex(metaDataBuilder, "foo", "bar");
        final RoleDescriptor roleMultiplePrivileges = new RoleDescriptor("roleMultiplePrivileges", new String[] { "manage_watcher" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges("write", "index2", "alias"),
                        indexPrivileges("read", "alias4"),
                        indexPrivileges("delete_index", "alias3", "index"),
                        indexPrivileges("create_index", "alias3", "index3")},
                null);
        DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
                mockClusterService(metaDataBuilder.build()), threadPool, deprecationLogger);
        deprecationConsumer.accept(Arrays.asList(roleMultiplePrivileges));
        verifyLogger(deprecationLogger, "roleMultiplePrivileges", "alias", "index, index3, index4");
        verifyLogger(deprecationLogger, "roleMultiplePrivileges", "alias4", "index2, index4");
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testPermissionsOverlapping() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        addIndex(metaDataBuilder, "index1", "alias1", "bar");
        addIndex(metaDataBuilder, "index2", "alias2", "baz");
        addIndex(metaDataBuilder, "foo", "bar");
        final RoleDescriptor roleOverAliasAndIndex = new RoleDescriptor("roleOverAliasAndIndex", new String[] { "read_ilm" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges("monitor", "index2", "alias1"),
                        indexPrivileges("monitor", "index1", "alias2")},
                null);
        DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
                mockClusterService(metaDataBuilder.build()), threadPool, deprecationLogger);
        deprecationConsumer.accept(Arrays.asList(roleOverAliasAndIndex));
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testOverlappingAcrossMultipleRoleDescriptors() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        addIndex(metaDataBuilder, "index1", "alias1", "bar");
        addIndex(metaDataBuilder, "index2", "alias2", "baz");
        addIndex(metaDataBuilder, "foo", "bar");
        final RoleDescriptor role1 = new RoleDescriptor("role1", new String[] { "monitor_watcher" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges("monitor", "index2", "alias1")},
                null);
        final RoleDescriptor role2 = new RoleDescriptor("role2", new String[] { "read_ccr" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges("monitor", "index1", "alias2")},
                null);
        final RoleDescriptor role3 = new RoleDescriptor("role3", new String[] { "monitor_ml" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges("index", "bar")},
                null);
        DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
                mockClusterService(metaDataBuilder.build()), threadPool, deprecationLogger);
        deprecationConsumer.accept(Arrays.asList(role1, role2, role3));
        verifyLogger(deprecationLogger, "role1", "alias1", "index1");
        verifyLogger(deprecationLogger, "role2", "alias2", "index2");
        verifyLogger(deprecationLogger, "role3", "bar", "foo, index1");
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testDailyRoleCaching() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        addIndex(metaDataBuilder, "index1", "alias1", "far");
        addIndex(metaDataBuilder, "index2", "alias2", "baz");
        addIndex(metaDataBuilder, "foo", "bar");
        final MetaData metaData = metaDataBuilder.build();
        RoleDescriptor someRole = new RoleDescriptor("someRole", new String[] { "monitor_rollup" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges("monitor", "i*", "bar")},
                null);
        final DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(mockClusterService(metaData),
                threadPool, deprecationLogger);
        final String cacheKeyBefore = DeprecationRoleDescriptorConsumer.buildCacheKey(someRole);
        deprecationConsumer.accept(Arrays.asList(someRole));
        verifyLogger(deprecationLogger, "someRole", "bar", "foo");
        verifyNoMoreInteractions(deprecationLogger);
        deprecationConsumer.accept(Arrays.asList(someRole));
        final String cacheKeyAfter = DeprecationRoleDescriptorConsumer.buildCacheKey(someRole);
        // we don't do this test if it crosses days
        if (false == cacheKeyBefore.equals(cacheKeyAfter)) {
            return;
        }
        verifyNoMoreInteractions(deprecationLogger);
        RoleDescriptor differentRoleSameName = new RoleDescriptor("someRole", new String[] { "manage_pipeline" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges("write", "i*", "baz")},
                null);
        deprecationConsumer.accept(Arrays.asList(differentRoleSameName));
        final String cacheKeyAfterParty = DeprecationRoleDescriptorConsumer.buildCacheKey(differentRoleSameName);
        // we don't do this test if it crosses days
        if (false == cacheKeyBefore.equals(cacheKeyAfterParty)) {
            return;
        }
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testWildcards() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        addIndex(metaDataBuilder, "index", "alias", "alias3");
        addIndex(metaDataBuilder, "index2", "alias", "alias2", "alias4");
        addIndex(metaDataBuilder, "index3", "alias", "alias3");
        addIndex(metaDataBuilder, "index4", "alias", "alias4");
        addIndex(metaDataBuilder, "foo", "bar", "baz");
        MetaData metaData = metaDataBuilder.build();
        final RoleDescriptor roleGlobalWildcard = new RoleDescriptor("roleGlobalWildcard", new String[] { "manage_token" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges(randomFrom("write", "delete_index", "read_cross_cluster"), "*")},
                null);
        new DeprecationRoleDescriptorConsumer(mockClusterService(metaData), threadPool, deprecationLogger)
        .accept(Arrays.asList(roleGlobalWildcard));
        verifyNoMoreInteractions(deprecationLogger);
        final RoleDescriptor roleGlobalWildcard2 = new RoleDescriptor("roleGlobalWildcard2", new String[] { "manage_index_templates" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges(randomFrom("write", "delete_index", "read_cross_cluster"), "i*", "a*")},
                null);
        new DeprecationRoleDescriptorConsumer(mockClusterService(metaData), threadPool, deprecationLogger)
        .accept(Arrays.asList(roleGlobalWildcard2));
        verifyNoMoreInteractions(deprecationLogger);
        final RoleDescriptor roleWildcardOnIndices = new RoleDescriptor("roleWildcardOnIndices", new String[] { "manage_watcher" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges("write", "index*", "alias", "alias3"),
                        indexPrivileges("read", "foo")},
                null);
        new DeprecationRoleDescriptorConsumer(mockClusterService(metaData), threadPool, deprecationLogger)
                .accept(Arrays.asList(roleWildcardOnIndices));
        verifyNoMoreInteractions(deprecationLogger);
        final RoleDescriptor roleWildcardOnAliases = new RoleDescriptor("roleWildcardOnAliases", new String[] { "manage_watcher" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges("write", "alias*", "index", "index3"),
                        indexPrivileges("read", "foo", "index2")},
                null);
        new DeprecationRoleDescriptorConsumer(mockClusterService(metaData), threadPool, deprecationLogger)
                .accept(Arrays.asList(roleWildcardOnAliases));
        verifyLogger(deprecationLogger, "roleWildcardOnAliases", "alias", "index2, index4");
        verifyLogger(deprecationLogger, "roleWildcardOnAliases", "alias2", "index2");
        verifyLogger(deprecationLogger, "roleWildcardOnAliases", "alias4", "index2, index4");
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testMultipleIndicesSameAlias() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        addIndex(metaDataBuilder, "index1", "alias1");
        addIndex(metaDataBuilder, "index2", "alias1", "alias2");
        addIndex(metaDataBuilder, "index3", "alias2");
        final RoleDescriptor roleOverAliasAndIndex = new RoleDescriptor("roleOverAliasAndIndex", new String[] { "manage_ml" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges("delete_index", "alias1", "index1") },
                null);
        DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
                mockClusterService(metaDataBuilder.build()), threadPool, deprecationLogger);
        deprecationConsumer.accept(Arrays.asList(roleOverAliasAndIndex));
        verifyLogger(deprecationLogger, "roleOverAliasAndIndex", "alias1", "index2");
        verifyNoMoreInteractions(deprecationLogger);
        final RoleDescriptor roleOverAliases = new RoleDescriptor("roleOverAliases", new String[] { "manage_security" },
                new RoleDescriptor.IndicesPrivileges[] {
                        indexPrivileges("monitor", "alias1", "alias2") },
                null);
        deprecationConsumer.accept(Arrays.asList(roleOverAliases));
        verifyLogger(deprecationLogger, "roleOverAliases", "alias1", "index1, index2");
        verifyLogger(deprecationLogger, "roleOverAliases", "alias2", "index2, index3");
        verifyNoMoreInteractions(deprecationLogger);
    }

    private void addIndex(MetaData.Builder metaDataBuilder, String index, String... aliases) {
        final IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(index)
                .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
                .numberOfShards(1)
                .numberOfReplicas(1);
        for (final String alias : aliases) {
            indexMetaDataBuilder.putAlias(AliasMetaData.builder(alias).build());
        }
        metaDataBuilder.put(indexMetaDataBuilder.build(), false);
    }

    private ClusterService mockClusterService(MetaData metaData) {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        when(clusterService.state()).thenReturn(clusterState);
        return clusterService;
    }

    private RoleDescriptor.IndicesPrivileges indexPrivileges(String priv, String... indicesOrAliases) {
        return RoleDescriptor.IndicesPrivileges.builder()
                .indices(indicesOrAliases)
                .privileges(priv)
                .grantedFields(randomArray(0, 2, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4)))
                .query(randomBoolean() ? null : "{ }")
                .build();
    }

    private void verifyLogger(DeprecationLogger deprecationLogger, String roleName, String aliasName, String indexNames) {
        verify(deprecationLogger).deprecated("Role [" + roleName + "] contains index privileges covering the [" + aliasName
                + "] alias but which do not cover some of the indices that it points to [" + indexNames + "]. Granting privileges over an"
                + " alias and hence granting privileges over all the indices that the alias points to is deprecated and will be removed"
                + " in a future version of Elasticsearch. Instead define permissions exclusively on index names or index name patterns.");
    }
}
