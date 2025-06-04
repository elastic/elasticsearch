/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TransportDeleteAutoscalingPolicyActionTests extends AutoscalingTestCase {

    public void testWriteBlock() {
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        final TransportDeleteAutoscalingPolicyAction action = new TransportDeleteAutoscalingPolicyAction(
            transportService,
            mock(ClusterService.class),
            threadPool,
            mock(ActionFilters.class)
        );
        final ClusterBlocks blocks = ClusterBlocks.builder()
            .addGlobalBlock(
                randomFrom(
                    Metadata.CLUSTER_READ_ONLY_BLOCK,
                    Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK,
                    NoMasterBlockService.NO_MASTER_BLOCK_WRITES
                )
            )
            .build();
        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(8))).blocks(blocks).build();
        final ClusterBlockException e = action.checkBlock(
            new DeleteAutoscalingPolicyAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, randomAlphaOfLength(8)),
            state
        );
        assertThat(e, not(nullValue()));
    }

    public void testNoWriteBlock() {
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        final TransportDeleteAutoscalingPolicyAction action = new TransportDeleteAutoscalingPolicyAction(
            transportService,
            mock(ClusterService.class),
            threadPool,
            mock(ActionFilters.class)
        );
        final ClusterBlocks blocks = ClusterBlocks.builder().build();
        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(8))).blocks(blocks).build();
        final ClusterBlockException e = action.checkBlock(
            new DeleteAutoscalingPolicyAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, randomAlphaOfLength(8)),
            state
        );
        assertThat(e, nullValue());
    }

    public void testDeletePolicy() {
        final ClusterState currentState;
        {
            final ClusterState.Builder builder = ClusterState.builder(new ClusterName(randomAlphaOfLength(8)));
            builder.metadata(
                Metadata.builder().putCustom(AutoscalingMetadata.NAME, randomAutoscalingMetadataOfPolicyCount(randomIntBetween(1, 8)))
            );
            currentState = builder.build();
        }
        final AutoscalingMetadata currentMetadata = currentState.metadata().custom(AutoscalingMetadata.NAME);
        final String name = randomFrom(currentMetadata.policies().keySet());
        final Logger mockLogger = mock(Logger.class);
        final ClusterState state = TransportDeleteAutoscalingPolicyAction.deleteAutoscalingPolicy(currentState, name, mockLogger);

        // ensure the policy is deleted from the cluster state
        final AutoscalingMetadata metadata = state.metadata().custom(AutoscalingMetadata.NAME);
        assertNotNull(metadata);
        assertThat(metadata.policies(), not(hasKey(name)));
        verify(mockLogger).info("deleting autoscaling policy [{}]", name);
        verifyNoMoreInteractions(mockLogger);

        // ensure that existing policies were otherwise preserved
        for (final Map.Entry<String, AutoscalingPolicyMetadata> entry : currentMetadata.policies().entrySet()) {
            if (entry.getKey().equals(name)) {
                continue;
            }
            assertThat(metadata.policies(), hasKey(entry.getKey()));
            assertThat(metadata.policies().get(entry.getKey()).policy(), equalTo(entry.getValue().policy()));
        }
    }

    public void testDeletePolicyByWildcard() {
        final ClusterState currentState;
        {
            final ClusterState.Builder builder = ClusterState.builder(new ClusterName(randomAlphaOfLength(8)));
            builder.metadata(
                Metadata.builder().putCustom(AutoscalingMetadata.NAME, randomAutoscalingMetadataOfPolicyCount(randomIntBetween(1, 8)))
            );
            currentState = builder.build();
        }
        final AutoscalingMetadata currentMetadata = currentState.metadata().custom(AutoscalingMetadata.NAME);
        final String policyName = randomFrom(currentMetadata.policies().keySet());
        final String deleteName = randomFrom(policyName.substring(0, between(0, policyName.length()))) + "*";
        final Logger mockLogger = mock(Logger.class);
        final ClusterState state = TransportDeleteAutoscalingPolicyAction.deleteAutoscalingPolicy(currentState, deleteName, mockLogger);

        // ensure the policy is deleted from the cluster state
        final AutoscalingMetadata metadata = state.metadata().custom(AutoscalingMetadata.NAME);
        assertNotNull(metadata);
        assertThat(metadata.policies(), not(hasKey(policyName)));

        verify(mockLogger).info("deleting [{}] autoscaling policies", currentMetadata.policies().size() - metadata.policies().size());
        verifyNoMoreInteractions(mockLogger);

        // ensure that the right policies were preserved
        for (final Map.Entry<String, AutoscalingPolicyMetadata> entry : currentMetadata.policies().entrySet()) {
            if (Regex.simpleMatch(deleteName, entry.getKey())) {
                assertFalse(metadata.policies().containsKey(entry.getKey()));
            } else {
                assertThat(metadata.policies(), hasKey(entry.getKey()));
                assertThat(metadata.policies().get(entry.getKey()).policy(), equalTo(entry.getValue().policy()));
            }
        }
    }

    public void testDeleteNonExistentPolicy() {
        final ClusterState currentState;
        {
            final ClusterState.Builder builder = ClusterState.builder(new ClusterName(randomAlphaOfLength(8)));
            builder.metadata(
                Metadata.builder().putCustom(AutoscalingMetadata.NAME, randomAutoscalingMetadataOfPolicyCount(randomIntBetween(1, 8)))
            );
            currentState = builder.build();
        }
        final AutoscalingMetadata currentMetadata = currentState.metadata().custom(AutoscalingMetadata.NAME);
        final String name = randomValueOtherThanMany(currentMetadata.policies().keySet()::contains, () -> randomAlphaOfLength(8));
        final Logger mockLogger = mock(Logger.class);
        final ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> TransportDeleteAutoscalingPolicyAction.deleteAutoscalingPolicy(currentState, name, mockLogger)
        );
        assertThat(e.getMessage(), containsString("autoscaling policy with name [" + name + "] does not exist"));
        verifyNoMoreInteractions(mockLogger);
    }

}
