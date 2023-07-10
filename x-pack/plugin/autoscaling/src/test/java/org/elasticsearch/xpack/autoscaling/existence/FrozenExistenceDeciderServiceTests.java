/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.existence;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;

import java.util.function.Consumer;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.autoscaling.existence.FrozenExistenceDeciderService.isFrozenPhase;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FrozenExistenceDeciderServiceTests extends AutoscalingTestCase {

    public void testScale() {
        verify(ClusterState.EMPTY_STATE, this::assertZeroCapacity);

        final Settings versionSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        final int shards = between(1, 3);
        final int replicas = between(0, 2);
        final Metadata nonFrozenMetadata = Metadata.builder()
            .put(IndexMetadata.builder("index").settings(versionSettings).numberOfShards(shards).numberOfReplicas(replicas))
            .build();
        verify(nonFrozenMetadata, this::assertZeroCapacity);

        final Metadata frozenMetadata = (randomBoolean() ? Metadata.builder() : Metadata.builder(nonFrozenMetadata)).put(
            IndexMetadata.builder("index")
                .settings(versionSettings)
                .putCustom(
                    LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY,
                    LifecycleExecutionState.builder().setPhase("frozen").build().asMap()
                )
                .numberOfShards(shards)
                .numberOfReplicas(replicas)
        ).build();
        verify(frozenMetadata, this::assertMinimumCapacity);
    }

    private void verify(Metadata metadata, Consumer<AutoscalingDeciderResult> resultConsumer) {
        verify(ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build(), resultConsumer);
    }

    private void verify(ClusterState state, Consumer<AutoscalingDeciderResult> resultConsumer) {
        FrozenExistenceDeciderService service = new FrozenExistenceDeciderService();
        AutoscalingDeciderContext context = mock(AutoscalingDeciderContext.class);
        when(context.state()).thenReturn(state);
        resultConsumer.accept(service.scale(Settings.EMPTY, context));
    }

    private void assertMinimumCapacity(AutoscalingDeciderResult result) {
        AutoscalingCapacity capacity = result.requiredCapacity();
        assertThat(capacity.total().memory(), equalTo(FrozenExistenceDeciderService.MINIMUM_FROZEN_MEMORY));
        assertThat(capacity.total().storage(), equalTo(FrozenExistenceDeciderService.MINIMUM_FROZEN_STORAGE));
        assertThat(capacity.node().memory(), equalTo(FrozenExistenceDeciderService.MINIMUM_FROZEN_MEMORY));
        assertThat(capacity.node().storage(), equalTo(FrozenExistenceDeciderService.MINIMUM_FROZEN_STORAGE));
        assertThat(result.reason().summary(), equalTo("indices [index]"));
    }

    private void assertZeroCapacity(AutoscalingDeciderResult result) {
        AutoscalingCapacity capacity = result.requiredCapacity();
        assertThat(capacity.total().memory(), equalTo(ByteSizeValue.ZERO));
        assertThat(capacity.total().storage(), equalTo(ByteSizeValue.ZERO));
        assertThat(capacity.node(), is(nullValue()));
        assertThat(result.reason().summary(), equalTo("indices []"));
    }

    public void testIsFrozenPhase() {
        assertThat(TimeseriesLifecycleType.ORDERED_VALID_PHASES, hasItem(FrozenExistenceDeciderService.FROZEN_PHASE));

        IndexMetadata meta0 = IndexMetadata.builder("index")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        assertFalse(isFrozenPhase(meta0));

        LifecycleExecutionState.Builder hot = LifecycleExecutionState.builder()
            .setPhase("hot")
            .setAction(randomAlphaOfLengthBetween(5, 20))
            .setAction(randomAlphaOfLengthBetween(5, 20));

        IndexMetadata meta1 = IndexMetadata.builder("index")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, hot.build().asMap())
            .build();
        assertFalse(isFrozenPhase(meta1));

        LifecycleExecutionState.Builder frozen = LifecycleExecutionState.builder()
            .setPhase(FrozenExistenceDeciderService.FROZEN_PHASE)
            .setAction(randomAlphaOfLengthBetween(5, 20))
            .setAction(randomAlphaOfLengthBetween(5, 20));

        IndexMetadata meta2 = IndexMetadata.builder("index")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, frozen.build().asMap())
            .build();
        assertTrue(isFrozenPhase(meta2));
    }
}
