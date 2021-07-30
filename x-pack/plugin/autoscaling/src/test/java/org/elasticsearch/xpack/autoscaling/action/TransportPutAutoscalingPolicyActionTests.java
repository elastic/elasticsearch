/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.autoscaling.AutoscalingLicenseChecker;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TransportPutAutoscalingPolicyActionTests extends AutoscalingTestCase {
    private static final PolicyValidator NO_VALIDATION = policy -> {};

    public void testWriteBlock() {
        final TransportPutAutoscalingPolicyAction action = new TransportPutAutoscalingPolicyAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            NO_VALIDATION,
            new AutoscalingLicenseChecker(() -> true)
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
        final ClusterBlockException e = action.checkBlock(randomPutAutoscalingPolicyRequest(), state);
        assertThat(e, not(nullValue()));
    }

    public void testNoWriteBlock() {
        final TransportPutAutoscalingPolicyAction action = new TransportPutAutoscalingPolicyAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            NO_VALIDATION,
            new AutoscalingLicenseChecker(() -> true)
        );
        final ClusterBlocks blocks = ClusterBlocks.builder().build();
        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(8))).blocks(blocks).build();
        final ClusterBlockException e = action.checkBlock(randomPutAutoscalingPolicyRequest(), state);
        assertThat(e, nullValue());
    }

    public void testAddPolicy() {
        final ClusterState currentState;
        {
            final ClusterState.Builder builder = ClusterState.builder(new ClusterName(randomAlphaOfLength(8)));
            if (randomBoolean()) {
                builder.metadata(Metadata.builder().putCustom(AutoscalingMetadata.NAME, randomAutoscalingMetadata()));
            }
            currentState = builder.build();
        }
        // put an entirely new policy
        final PutAutoscalingPolicyAction.Request request = randomPutAutoscalingPolicyRequest();
        final Logger mockLogger = mock(Logger.class);
        final ClusterState state = TransportPutAutoscalingPolicyAction.putAutoscalingPolicy(
            currentState,
            request,
            NO_VALIDATION,
            mockLogger
        );

        // ensure the new policy is in the updated cluster state
        final AutoscalingMetadata metadata = state.metadata().custom(AutoscalingMetadata.NAME);
        assertNotNull(metadata);
        assertThat(metadata.policies(), hasKey(request.name()));
        assertThat(metadata.policies().get(request.name()).policy().roles(), equalTo(request.roles()));
        if (request.deciders() != null) {
            assertThat(metadata.policies().get(request.name()).policy().deciders(), equalTo(request.deciders()));
        } else {
            assertThat(metadata.policies().get(request.name()).policy().deciders(), equalTo(Map.of()));
        }
        verify(mockLogger).info("adding autoscaling policy [{}]", request.name());
        verifyNoMoreInteractions(mockLogger);

        // ensure that existing policies were preserved
        final AutoscalingMetadata currentMetadata = currentState.metadata().custom(AutoscalingMetadata.NAME);
        if (currentMetadata != null) {
            for (final Map.Entry<String, AutoscalingPolicyMetadata> entry : currentMetadata.policies().entrySet()) {
                assertThat(metadata.policies(), hasKey(entry.getKey()));
                assertThat(metadata.policies().get(entry.getKey()).policy(), equalTo(entry.getValue().policy()));
            }
        }
    }

    public void testAddPolicyWithNoRoles() {
        PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            randomAlphaOfLength(8),
            null,
            randomAutoscalingDeciders()
        );

        final Logger mockLogger = mock(Logger.class);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TransportPutAutoscalingPolicyAction.putAutoscalingPolicy(ClusterState.EMPTY_STATE, request, NO_VALIDATION, mockLogger)
        );
        assertThat(
            exception.getMessage(),
            equalTo("new policy " + request.name() + " with no roles defined, must provide empty list for " + "no roles")
        );
    }

    public void testUpdatePolicy() {
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
        // add to the existing deciders, to ensure the policy has changed
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            name,
            randomBoolean() ? randomRoles() : null,
            mutateAutoscalingDeciders(currentMetadata.policies().get(name).policy().deciders())
        );
        final AutoscalingPolicy expectedPolicy = new AutoscalingPolicy(
            name,
            request.roles() != null ? request.roles() : currentMetadata.policies().get(name).policy().roles(),
            request.deciders()
        );
        final Logger mockLogger = mock(Logger.class);
        final ClusterState state = TransportPutAutoscalingPolicyAction.putAutoscalingPolicy(
            currentState,
            request,
            NO_VALIDATION,
            mockLogger
        );

        // ensure the updated policy is in the updated cluster state
        final AutoscalingMetadata metadata = state.metadata().custom(AutoscalingMetadata.NAME);
        assertNotNull(metadata);
        assertThat(metadata.policies(), hasKey(request.name()));
        assertThat(metadata.policies().get(request.name()).policy(), equalTo(expectedPolicy));
        verify(mockLogger).info("updating autoscaling policy [{}]", request.name());
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

    public void testNoOpUpdatePolicy() {
        final ClusterState currentState;
        {
            final ClusterState.Builder builder = ClusterState.builder(new ClusterName(randomAlphaOfLength(8)));
            builder.metadata(
                Metadata.builder().putCustom(AutoscalingMetadata.NAME, randomAutoscalingMetadataOfPolicyCount(randomIntBetween(1, 8)))
            );
            currentState = builder.build();
        }
        // randomly put an existing policy
        final AutoscalingMetadata currentMetadata = currentState.metadata().custom(AutoscalingMetadata.NAME);
        final AutoscalingPolicy policy = randomFrom(currentMetadata.policies().values()).policy();
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            policy.name(),
            randomBoolean() ? policy.roles() : null,
            randomBoolean() ? policy.deciders() : null
        );
        final Logger mockLogger = mock(Logger.class);
        final ClusterState state = TransportPutAutoscalingPolicyAction.putAutoscalingPolicy(
            currentState,
            request,
            NO_VALIDATION,
            mockLogger
        );

        assertThat(state, sameInstance(currentState));
        verify(mockLogger).info("skipping updating autoscaling policy [{}] due to no change in policy", policy.name());
        verifyNoMoreInteractions(mockLogger);
    }

    public void testPolicyValidator() {
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            randomAlphaOfLength(8),
            randomRoles(),
            Collections.emptySortedMap()
        );
        final Logger mockLogger = mock(Logger.class);
        expectThrows(
            IllegalArgumentException.class,
            () -> TransportPutAutoscalingPolicyAction.putAutoscalingPolicy(
                ClusterState.EMPTY_STATE,
                request,
                p -> { throw new IllegalArgumentException(); },
                mockLogger
            )
        );

        verifyNoMoreInteractions(mockLogger);
    }

    static PutAutoscalingPolicyAction.Request randomPutAutoscalingPolicyRequest() {
        return new PutAutoscalingPolicyAction.Request(
            randomAlphaOfLength(8),
            randomRoles(),
            randomBoolean() ? randomAutoscalingDeciders() : null
        );
    }
}
