/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.indices.SystemIndexDescriptor.VERSION_META_KEY;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SystemIndexMigrationTaskExecutorTests extends ESTestCase {
    private ThreadPool threadPool;
    private Client client;
    private SystemIndices systemIndices;

    private int updateIndexMigrationVersionActionInvocations;

    @Before
    public void setUpMocks() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);

        updateIndexMigrationVersionActionInvocations = 0;
        systemIndices = mock(SystemIndices.class);
        client = new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                updateIndexMigrationVersionActionInvocations++;
                listener.onResponse((Response) new UpdateIndexMigrationVersionResponse());
            }
        };
    }

    public SystemIndexDescriptor.Builder testDescriptorBuilder(String indexName) throws IOException {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(".test-[0-9]+*")
            .setSettings(Settings.EMPTY)
            .setVersionMetaKey(VERSION_META_KEY)
            .setOrigin("test")
            .setMappings(jsonBuilder().startObject().startObject("_meta").field(VERSION_META_KEY, 1).endObject().endObject())
            .setPrimaryIndex(indexName);
    }

    public void testSingleSuccessfulMigration() throws IOException {
        final int[] migrateInvocations = new int[1];
        SystemIndexMigrationTaskExecutor systemMigrationExecutor = new SystemIndexMigrationTaskExecutor(
            threadPool.generic(),
            client,
            systemIndices
        );

        String indexName = ".test-1";
        SystemIndexDescriptor systemIndexDescriptor = testDescriptorBuilder(indexName).addSystemIndexMigration(
            generateMigration(migrateInvocations)
        ).build();

        when(systemIndices.findMatchingDescriptor(indexName)).thenReturn(systemIndexDescriptor);

        AllocatedPersistentTask mockTask = mock(AllocatedPersistentTask.class);
        systemMigrationExecutor.nodeOperation(
            mockTask,
            new SystemIndexMigrationTaskParams(indexName, new int[] { 1 }),
            mock(PersistentTaskState.class)
        );

        verify(mockTask, times(1)).markAsCompleted();
        verify(mockTask, times(0)).markAsFailed(any());
        assertEquals(1, updateIndexMigrationVersionActionInvocations);
        assertEquals(1, migrateInvocations[0]);
    }

    public void testSeveralSuccessfulMigrations() throws IOException {
        final int[] migrateInvocations = new int[1];
        SystemIndexMigrationTaskExecutor systemMigrationExecutor = new SystemIndexMigrationTaskExecutor(
            threadPool.generic(),
            client,
            systemIndices
        );

        String indexName = ".test-1";
        SystemIndexDescriptor systemIndexDescriptor = testDescriptorBuilder(indexName).addSystemIndexMigration(
            generateMigration(migrateInvocations)
        )
            .addSystemIndexMigration(generateMigration(migrateInvocations))
            .addSystemIndexMigration(generateMigration(migrateInvocations))
            .build();

        when(systemIndices.findMatchingDescriptor(indexName)).thenReturn(systemIndexDescriptor);

        AllocatedPersistentTask mockTask = mock(AllocatedPersistentTask.class);
        systemMigrationExecutor.nodeOperation(
            mockTask,
            new SystemIndexMigrationTaskParams(indexName, new int[] { 1, 2, 3 }),
            mock(PersistentTaskState.class)
        );

        verify(mockTask, times(1)).markAsCompleted();
        verify(mockTask, times(0)).markAsFailed(any());
        assertEquals(3, updateIndexMigrationVersionActionInvocations);
        assertEquals(3, migrateInvocations[0]);
    }

    public void testPartialMigration() throws IOException {
        final int[] migrateInvocations = new int[1];
        SystemIndexMigrationTaskExecutor systemMigrationExecutor = new SystemIndexMigrationTaskExecutor(
            threadPool.generic(),
            client,
            systemIndices
        );

        String indexName = ".test-1";
        SystemIndexDescriptor systemIndexDescriptor = testDescriptorBuilder(indexName).addSystemIndexMigration(
            generateMigration(migrateInvocations)
        )
            .addSystemIndexMigration(generateMigration(migrateInvocations))
            .addSystemIndexMigration(generateMigration(migrateInvocations))
            .build();

        when(systemIndices.findMatchingDescriptor(indexName)).thenReturn(systemIndexDescriptor);

        AllocatedPersistentTask mockTask = mock(AllocatedPersistentTask.class);
        systemMigrationExecutor.nodeOperation(
            mockTask,
            new SystemIndexMigrationTaskParams(indexName, new int[] { 1, 2 }),
            mock(PersistentTaskState.class)
        );

        verify(mockTask, times(1)).markAsCompleted();
        verify(mockTask, times(0)).markAsFailed(any());
        assertEquals(2, updateIndexMigrationVersionActionInvocations);
        assertEquals(2, migrateInvocations[0]);
    }

    private SystemIndexMigrationTask generateMigration(int[] migrateInvocationsCounter) {
        return new SystemIndexMigrationTask() {
            @Override
            public void migrate(ActionListener<Void> listener) {
                migrateInvocationsCounter[0]++;
                listener.onResponse(null);
            }

            @Override
            public Set<NodeFeature> nodeFeaturesRequired() {
                return Set.of();
            }

            @Override
            public int minMappingVersion() {
                return 0;
            }

            @Override
            public boolean checkPreConditions() {
                return true;
            }
        };
    }
}
