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

import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class DeprecationRoleDescriptorConsumerTests extends ESTestCase {

    private DeprecationLogger deprecationLogger;
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        this.deprecationLogger = mock(DeprecationLogger.class);
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

    public void testSimpleAliasIndexPair() throws Exception {
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        addIndex(metaDataBuilder, "index", "alias");
        final RoleDescriptor roleDescriptor = new RoleDescriptor("role1", null,
                new RoleDescriptor.IndicesPrivileges[] { indexPrivileges("read", "alias") }, null);
        DeprecationRoleDescriptorConsumer deprecation = new DeprecationRoleDescriptorConsumer(mockClusterService(metaDataBuilder.build()),
                this.threadPool, deprecationLogger);
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
                .query(randomFrom(null, "{ }"))
                .build();
    }

}
