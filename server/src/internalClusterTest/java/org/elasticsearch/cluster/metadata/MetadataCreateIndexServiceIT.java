/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MetadataCreateIndexServiceIT extends ESIntegTestCase {

    public void testRequestTemplateIsRespected() throws InterruptedException {
        /*
         * This test passes a template in the CreateIndexClusterStateUpdateRequest, and makes sure that the settings
         * from that template are used when creating the index.
         */
        MetadataCreateIndexService metadataCreateIndexService = internalCluster().getCurrentMasterNodeInstance(
            MetadataCreateIndexService.class
        );
        final String indexName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        final int numberOfReplicas = randomIntBetween(1, 7);
        CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest(
            "testRequestTemplateIsRespected",
            ProjectId.DEFAULT,
            indexName,
            randomAlphaOfLength(20)
        );
        request.setMatchingTemplate(
            ComposableIndexTemplate.builder()
                .template(Template.builder().settings(Settings.builder().put("index.number_of_replicas", numberOfReplicas)))
                .build()
        );
        final CountDownLatch listenerCalledLatch = new CountDownLatch(1);
        ActionListener<ShardsAcknowledgedResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(ShardsAcknowledgedResponse shardsAcknowledgedResponse) {
                listenerCalledLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(e);
                listenerCalledLatch.countDown();
            }
        };

        metadataCreateIndexService.createIndex(
            TimeValue.THIRTY_SECONDS,
            TimeValue.THIRTY_SECONDS,
            TimeValue.THIRTY_SECONDS,
            request,
            listener
        );
        listenerCalledLatch.await(10, TimeUnit.SECONDS);
        GetIndexResponse response = admin().indices()
            .getIndex(new GetIndexRequest(TimeValue.THIRTY_SECONDS).indices(indexName))
            .actionGet();
        Settings settings = response.getSettings().get(indexName);
        assertThat(settings.get("index.number_of_replicas"), equalTo(Integer.toString(numberOfReplicas)));
    }

    public void testCreateIndexBatching() throws Exception {
        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);

        final int totalRequestCount = randomIntBetween(1, 20);
        final int validRequestCount = randomIntBetween(0, totalRequestCount);
        final int invalidRequestCount = totalRequestCount - validRequestCount;

        final var allRequestNames = new ArrayList<String>(totalRequestCount);
        final var validIndicesNames = new LinkedHashSet<String>();
        final var invalidSettingsNames = new LinkedHashSet<String>();

        for (int i = 0; i < validRequestCount; i++) {
            final var indexName = randomIndexName();
            validIndicesNames.add(indexName);
            allRequestNames.add(indexName);
        }
        // No collisions
        assertThat(validIndicesNames.size(), equalTo(validRequestCount));

        int duplicateCount = 0;
        int invalidNameCount = 0;
        int invalidSettingsCount = 0;
        for (int i = 0; i < invalidRequestCount; i++) {
            int failureType = validIndicesNames.isEmpty() ? randomIntBetween(0, 1) : randomIntBetween(0, 2);
            switch (failureType) {
                case 0 -> {
                    allRequestNames.add("INVALID_" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT));
                    invalidNameCount++;
                }
                case 1 -> {
                    final var indexName = randomIndexName();
                    invalidSettingsNames.add(indexName);
                    allRequestNames.add(indexName);
                    invalidSettingsCount++;
                }
                default -> {
                    allRequestNames.add(randomFrom(validIndicesNames));
                    duplicateCount++;
                }
            }
        }
        Collections.shuffle(allRequestNames, random());

        final ClusterStateListener listener = event -> {
            final var projectMetadata = event.state().metadata().getProject(ProjectId.DEFAULT);
            if (projectMetadata == null) {
                return;
            }
            final var createdInState = validIndicesNames.stream().filter(projectMetadata::hasIndex).toList();
            assertThat(
                "expected either none or all valid indices to appear atomically, but found " + createdInState.size(),
                createdInState,
                anyOf(hasSize(0), hasSize(validIndicesNames.size()))
            );
            for (final var indexName : invalidSettingsNames) {
                assertFalse("invalid index [" + indexName + "] should never be created", projectMetadata.hasIndex(indexName));
            }
        };
        masterClusterService.addListener(listener);
        try {
            final var barrier = new CyclicBarrier(2);
            masterClusterService.submitUnbatchedStateUpdateTask("block", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    safeAwait(barrier);
                    safeAwait(barrier);
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("blocking task failed", e);
                }
            });
            safeAwait(barrier);

            final var futures = new ArrayList<ActionFuture<CreateIndexResponse>>(totalRequestCount);
            for (final var indexName : allRequestNames) {
                final var request = new CreateIndexRequest(indexName);
                if (invalidSettingsNames.contains(indexName)) {
                    request.settings(Settings.builder().put("index.version.created", 1));
                }
                futures.add(client().execute(TransportCreateIndexAction.TYPE, request));
            }

            assertTrue(
                "create-index tasks should be queued behind the blocking task",
                waitUntil(
                    () -> masterClusterService.getMasterService()
                        .pendingTasks()
                        .stream()
                        .filter(pct -> pct.getSource().toString().startsWith("create-index"))
                        .count() == totalRequestCount
                )
            );
            safeAwait(barrier);

            int successCount = 0;
            int invalidNameExceptionCount = 0;
            int alreadyExistsExceptionCount = 0;
            int indexCreationExceptionCount = 0;
            for (final var future : futures) {
                try {
                    assertTrue(future.get(30, TimeUnit.SECONDS).isAcknowledged());
                    successCount++;
                } catch (ExecutionException e) {
                    final var cause = ExceptionsHelper.unwrapCause(e.getCause());
                    switch (cause) {
                        case InvalidIndexNameException ignored -> invalidNameExceptionCount++;
                        case ResourceAlreadyExistsException ignored -> alreadyExistsExceptionCount++;
                        case IllegalArgumentException ignored -> indexCreationExceptionCount++;
                        case null, default -> throw new AssertionError("Unexpected failure when creating indices in batch", e);
                    }
                }
            }
            assertThat(successCount, equalTo(validIndicesNames.size()));
            assertThat(invalidNameExceptionCount, equalTo(invalidNameCount));
            assertThat(alreadyExistsExceptionCount, equalTo(duplicateCount));
            assertThat(indexCreationExceptionCount, equalTo(invalidSettingsCount));
        } finally {
            masterClusterService.removeListener(listener);
        }
    }
}
