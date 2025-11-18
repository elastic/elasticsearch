/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        addIndex(projectBuilder, "index", "alias");
        final RoleDescriptor roleOverAlias = new RoleDescriptor(
            "roleOverAlias",
            new String[] { "read" },
            new RoleDescriptor.IndicesPrivileges[] { indexPrivileges(randomFrom("read", "write", "delete", "index"), "alias") },
            null
        );
        final RoleDescriptor roleOverIndex = new RoleDescriptor(
            "roleOverIndex",
            new String[] { "manage" },
            new RoleDescriptor.IndicesPrivileges[] { indexPrivileges(randomFrom("read", "write", "delete", "index"), "index") },
            null
        );
        DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
            mockClusterService(projectBuilder),
            TestProjectResolvers.singleProject(projectBuilder.getId()),
            threadPool,
            deprecationLogger
        );
        deprecationConsumer.accept(Arrays.asList(roleOverAlias, roleOverIndex));
        verifyLogger(deprecationLogger, "roleOverAlias", "alias", "index");
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testRoleGrantsOnIndexAndAliasPair() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        addIndex(projectBuilder, "index", "alias");
        addIndex(projectBuilder, "index1", "alias2");
        final RoleDescriptor roleOverIndexAndAlias = new RoleDescriptor(
            "roleOverIndexAndAlias",
            new String[] { "manage_watcher" },
            new RoleDescriptor.IndicesPrivileges[] { indexPrivileges(randomFrom("read", "write", "delete", "index"), "index", "alias") },
            null
        );
        DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
            mockClusterService(projectBuilder),
            TestProjectResolvers.singleProject(projectBuilder.getId()),
            threadPool,
            deprecationLogger
        );
        deprecationConsumer.accept(Arrays.asList(roleOverIndexAndAlias));
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testMultiplePrivilegesLoggedOnce() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        addIndex(projectBuilder, "index", "alias");
        addIndex(projectBuilder, "index2", "alias2");
        final RoleDescriptor roleOverAlias = new RoleDescriptor(
            "roleOverAlias",
            new String[] { "manage_watcher" },
            new RoleDescriptor.IndicesPrivileges[] { indexPrivileges("write", "alias"), indexPrivileges("manage_ilm", "alias") },
            null
        );
        DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
            mockClusterService(projectBuilder),
            TestProjectResolvers.singleProject(projectBuilder.getId()),
            threadPool,
            deprecationLogger
        );
        deprecationConsumer.accept(Arrays.asList(roleOverAlias));
        verifyLogger(deprecationLogger, "roleOverAlias", "alias", "index");
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testMultiplePrivilegesLoggedForEachAlias() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        addIndex(projectBuilder, "index", "alias", "alias3");
        addIndex(projectBuilder, "index2", "alias2", "alias", "alias4");
        addIndex(projectBuilder, "index3", "alias3", "alias");
        addIndex(projectBuilder, "index4", "alias4", "alias");
        addIndex(projectBuilder, "foo", "bar");
        final RoleDescriptor roleMultiplePrivileges = new RoleDescriptor(
            "roleMultiplePrivileges",
            new String[] { "manage_watcher" },
            new RoleDescriptor.IndicesPrivileges[] {
                indexPrivileges("write", "index2", "alias"),
                indexPrivileges("read", "alias4"),
                indexPrivileges("delete_index", "alias3", "index"),
                indexPrivileges("create_index", "alias3", "index3") },
            null
        );
        DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
            mockClusterService(projectBuilder),
            TestProjectResolvers.singleProject(projectBuilder.getId()),
            threadPool,
            deprecationLogger
        );
        deprecationConsumer.accept(Arrays.asList(roleMultiplePrivileges));
        verifyLogger(deprecationLogger, "roleMultiplePrivileges", "alias", "index, index3, index4");
        verifyLogger(deprecationLogger, "roleMultiplePrivileges", "alias4", "index2, index4");
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testPermissionsOverlapping() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        addIndex(projectBuilder, "index1", "alias1", "bar");
        addIndex(projectBuilder, "index2", "alias2", "baz");
        addIndex(projectBuilder, "foo", "bar");
        final RoleDescriptor roleOverAliasAndIndex = new RoleDescriptor(
            "roleOverAliasAndIndex",
            new String[] { "read_ilm" },
            new RoleDescriptor.IndicesPrivileges[] {
                indexPrivileges("monitor", "index2", "alias1"),
                indexPrivileges("monitor", "index1", "alias2") },
            null
        );
        DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
            mockClusterService(projectBuilder),
            TestProjectResolvers.singleProject(projectBuilder.getId()),
            threadPool,
            deprecationLogger
        );
        deprecationConsumer.accept(Arrays.asList(roleOverAliasAndIndex));
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testOverlappingAcrossMultipleRoleDescriptors() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        addIndex(projectBuilder, "index1", "alias1", "bar");
        addIndex(projectBuilder, "index2", "alias2", "baz");
        addIndex(projectBuilder, "foo", "bar");
        final RoleDescriptor role1 = new RoleDescriptor(
            "role1",
            new String[] { "monitor_watcher" },
            new RoleDescriptor.IndicesPrivileges[] { indexPrivileges("monitor", "index2", "alias1") },
            null
        );
        final RoleDescriptor role2 = new RoleDescriptor(
            "role2",
            new String[] { "read_ccr" },
            new RoleDescriptor.IndicesPrivileges[] { indexPrivileges("monitor", "index1", "alias2") },
            null
        );
        final RoleDescriptor role3 = new RoleDescriptor(
            "role3",
            new String[] { "monitor_ml" },
            new RoleDescriptor.IndicesPrivileges[] { indexPrivileges("index", "bar") },
            null
        );
        DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
            mockClusterService(projectBuilder),
            TestProjectResolvers.singleProject(projectBuilder.getId()),
            threadPool,
            deprecationLogger
        );
        deprecationConsumer.accept(Arrays.asList(role1, role2, role3));
        verifyLogger(deprecationLogger, "role1", "alias1", "index1");
        verifyLogger(deprecationLogger, "role2", "alias2", "index2");
        verifyLogger(deprecationLogger, "role3", "bar", "foo, index1");
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testDailyRoleCaching() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        addIndex(projectBuilder, "index1", "alias1", "far");
        addIndex(projectBuilder, "index2", "alias2", "baz");
        addIndex(projectBuilder, "foo", "bar");
        RoleDescriptor someRole = new RoleDescriptor(
            "someRole",
            new String[] { "monitor_rollup" },
            new RoleDescriptor.IndicesPrivileges[] { indexPrivileges("monitor", "i*", "bar") },
            null
        );
        final DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
            mockClusterService(projectBuilder),
            TestProjectResolvers.singleProject(projectBuilder.getId()),
            threadPool,
            deprecationLogger
        );
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
        RoleDescriptor differentRoleSameName = new RoleDescriptor(
            "someRole",
            new String[] { "manage_pipeline" },
            new RoleDescriptor.IndicesPrivileges[] { indexPrivileges("write", "i*", "baz") },
            null
        );
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
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        addIndex(projectBuilder, "index", "alias", "alias3");
        addIndex(projectBuilder, "index2", "alias", "alias2", "alias4");
        addIndex(projectBuilder, "index3", "alias", "alias3");
        addIndex(projectBuilder, "index4", "alias", "alias4");
        addIndex(projectBuilder, "foo", "bar", "baz");
        final RoleDescriptor roleGlobalWildcard = new RoleDescriptor(
            "roleGlobalWildcard",
            new String[] { "manage_token" },
            new RoleDescriptor.IndicesPrivileges[] { indexPrivileges(randomFrom("write", "delete_index", "read_cross_cluster"), "*") },
            null
        );
        final ClusterService clusterService = mockClusterService(projectBuilder);
        final ProjectResolver projectResolver = TestProjectResolvers.singleProject(projectBuilder.getId());
        new DeprecationRoleDescriptorConsumer(clusterService, projectResolver, threadPool, deprecationLogger).accept(
            Arrays.asList(roleGlobalWildcard)
        );
        verifyNoMoreInteractions(deprecationLogger);
        final RoleDescriptor roleGlobalWildcard2 = new RoleDescriptor(
            "roleGlobalWildcard2",
            new String[] { "manage_index_templates" },
            new RoleDescriptor.IndicesPrivileges[] {
                indexPrivileges(randomFrom("write", "delete_index", "read_cross_cluster"), "i*", "a*") },
            null
        );
        new DeprecationRoleDescriptorConsumer(clusterService, projectResolver, threadPool, deprecationLogger).accept(
            Arrays.asList(roleGlobalWildcard2)
        );
        verifyNoMoreInteractions(deprecationLogger);
        final RoleDescriptor roleWildcardOnIndices = new RoleDescriptor(
            "roleWildcardOnIndices",
            new String[] { "manage_watcher" },
            new RoleDescriptor.IndicesPrivileges[] {
                indexPrivileges("write", "index*", "alias", "alias3"),
                indexPrivileges("read", "foo") },
            null
        );
        new DeprecationRoleDescriptorConsumer(clusterService, projectResolver, threadPool, deprecationLogger).accept(
            Arrays.asList(roleWildcardOnIndices)
        );
        verifyNoMoreInteractions(deprecationLogger);
        final RoleDescriptor roleWildcardOnAliases = new RoleDescriptor(
            "roleWildcardOnAliases",
            new String[] { "manage_watcher" },
            new RoleDescriptor.IndicesPrivileges[] {
                indexPrivileges("write", "alias*", "index", "index3"),
                indexPrivileges("read", "foo", "index2") },
            null
        );
        new DeprecationRoleDescriptorConsumer(clusterService, projectResolver, threadPool, deprecationLogger).accept(
            Arrays.asList(roleWildcardOnAliases)
        );
        verifyLogger(deprecationLogger, "roleWildcardOnAliases", "alias", "index2, index4");
        verifyLogger(deprecationLogger, "roleWildcardOnAliases", "alias2", "index2");
        verifyLogger(deprecationLogger, "roleWildcardOnAliases", "alias4", "index2, index4");
        verifyNoMoreInteractions(deprecationLogger);
    }

    public void testMultipleIndicesSameAlias() throws Exception {
        final DeprecationLogger deprecationLogger = mock(DeprecationLogger.class);
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        addIndex(projectBuilder, "index1", "alias1");
        addIndex(projectBuilder, "index2", "alias1", "alias2");
        addIndex(projectBuilder, "index3", "alias2");
        final RoleDescriptor roleOverAliasAndIndex = new RoleDescriptor(
            "roleOverAliasAndIndex",
            new String[] { "manage_ml" },
            new RoleDescriptor.IndicesPrivileges[] { indexPrivileges("delete_index", "alias1", "index1") },
            null
        );
        DeprecationRoleDescriptorConsumer deprecationConsumer = new DeprecationRoleDescriptorConsumer(
            mockClusterService(projectBuilder),
            TestProjectResolvers.singleProject(projectBuilder.getId()),
            threadPool,
            deprecationLogger
        );
        deprecationConsumer.accept(Arrays.asList(roleOverAliasAndIndex));
        verifyLogger(deprecationLogger, "roleOverAliasAndIndex", "alias1", "index2");
        verifyNoMoreInteractions(deprecationLogger);
        final RoleDescriptor roleOverAliases = new RoleDescriptor(
            "roleOverAliases",
            new String[] { "manage_security" },
            new RoleDescriptor.IndicesPrivileges[] { indexPrivileges("monitor", "alias1", "alias2") },
            null
        );
        deprecationConsumer.accept(Arrays.asList(roleOverAliases));
        verifyLogger(deprecationLogger, "roleOverAliases", "alias1", "index1, index2");
        verifyLogger(deprecationLogger, "roleOverAliases", "alias2", "index2, index3");
        verifyNoMoreInteractions(deprecationLogger);
    }

    private void addIndex(ProjectMetadata.Builder projectBuilder, String index, String... aliases) {
        final IndexMetadata.Builder indexprojectBuilder = IndexMetadata.builder(index)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersionUtils.randomVersion()))
            .numberOfShards(1)
            .numberOfReplicas(1);
        for (final String alias : aliases) {
            indexprojectBuilder.putAlias(AliasMetadata.builder(alias).build());
        }
        projectBuilder.put(indexprojectBuilder.build(), false);
    }

    private ClusterService mockClusterService(ProjectMetadata.Builder project) {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(project)).build();
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
        verify(deprecationLogger).warn(
            DeprecationCategory.SECURITY,
            "index_permissions_on_alias",
            "Role ["
                + roleName
                + "] contains index privileges covering the ["
                + aliasName
                + "] alias but which do not cover some of the indices that it points to ["
                + indexNames
                + "]. Granting privileges over an"
                + " alias and hence granting privileges over all the indices that the alias points to is deprecated and will be removed"
                + " in a future version of Elasticsearch. Instead define permissions exclusively on index names or index name patterns."
        );
    }
}
