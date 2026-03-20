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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
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

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoSuccessListener;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

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
        final var validIndexNames = new HashSet<String>();
        final var invalidSettingsNames = new HashSet<String>();
        final var allIndexNames = new HashSet<String>();

        final var preExistingIndexName = addRandomIndexNameNoCollision(allIndexNames);
        createIndex(preExistingIndexName);

        final ClusterStateListener listener = event -> {
            final var projectMetadata = event.state().metadata().getProject(ProjectId.DEFAULT);
            if (projectMetadata == null) {
                return;
            }
            final var createdInState = validIndexNames.stream().filter(projectMetadata::hasIndex).toList();
            assertThat(
                "expected either none or all valid indices to appear atomically, but found " + createdInState.size(),
                createdInState,
                anyOf(hasSize(0), hasSize(validIndexNames.size()))
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

            final var responsesLatch = new CountDownLatch(totalRequestCount);
            for (int i = 0; i < totalRequestCount; i++) {
                switch (randomIntBetween(0, 3)) {
                    // valid request
                    case 0 -> {
                        final var indexName = addRandomIndexNameNoCollision(allIndexNames);
                        validIndexNames.add(indexName);
                        client().execute(
                            TransportCreateIndexAction.TYPE,
                            new CreateIndexRequest(indexName),
                            new LatchedActionListener<>(
                                assertNoFailureListener(response -> assertTrue(response.isAcknowledged())),
                                responsesLatch
                            )
                        );
                    }
                    // invalid index name
                    case 1 -> client().execute(
                        TransportCreateIndexAction.TYPE,
                        new CreateIndexRequest(randomIdentifier("INVALID_BECAUSE_UPPER_CASE_")),
                        new LatchedActionListener<>(
                            assertNoSuccessListener(
                                e -> assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(InvalidIndexNameException.class))
                            ),
                            responsesLatch
                        )
                    );
                    // invalid settings
                    case 2 -> {
                        final var indexName = addRandomIndexNameNoCollision(allIndexNames);
                        invalidSettingsNames.add(indexName);
                        client().execute(
                            TransportCreateIndexAction.TYPE,
                            new CreateIndexRequest(indexName).settings(Settings.builder().put("index.version.created", 1)),
                            new LatchedActionListener<>(
                                assertNoSuccessListener(
                                    e -> assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(IllegalArgumentException.class))
                                ),
                                responsesLatch
                            )
                        );
                    }
                    // already existing index
                    default -> client().execute(
                        TransportCreateIndexAction.TYPE,
                        new CreateIndexRequest(preExistingIndexName),
                        new LatchedActionListener<>(
                            assertNoSuccessListener(
                                e -> assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(ResourceAlreadyExistsException.class))
                            ),
                            responsesLatch
                        )
                    );
                }
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
            final var initialState = masterClusterService.state();
            safeAwait(barrier);

            assertTrue("timed out waiting for create-index responses", responsesLatch.await(30, TimeUnit.SECONDS));
            if (validIndexNames.isEmpty()) {
                assertSame("cluster state should not change when all requests failed", masterClusterService.state(), initialState);
            }
        } finally {
            masterClusterService.removeListener(listener);
        }
    }

    private String addRandomIndexNameNoCollision(Set<String> existingIndexNames) {
        var indexName = randomIndexName();
        while (existingIndexNames.add(indexName) == false) {
            indexName = randomIndexName();
        }
        return indexName;
    }
}
