/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;

import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createTimestampField;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.hamcrest.Matchers.is;

public class ReplaceDataStreamBackingIndexStepTests extends AbstractStepTestCase<ReplaceDataStreamBackingIndexStep> {

    @Override
    protected ReplaceDataStreamBackingIndexStep createRandomInstance() {
        String prefix = randomAlphaOfLengthBetween(1, 10);
        return new ReplaceDataStreamBackingIndexStep(randomStepKey(), randomStepKey(), (index, state) -> prefix + index);
    }

    @Override
    protected ReplaceDataStreamBackingIndexStep mutateInstance(ReplaceDataStreamBackingIndexStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();
        BiFunction<String, LifecycleExecutionState, String> indexNameSupplier = instance.getTargetIndexNameSupplier();

        switch (between(0, 2)) {
            case 0:
                key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 2:
                indexNameSupplier = (index, state) -> randomAlphaOfLengthBetween(11, 15) + index;
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new ReplaceDataStreamBackingIndexStep(key, nextKey, indexNameSupplier);
    }

    @Override
    protected ReplaceDataStreamBackingIndexStep copyInstance(ReplaceDataStreamBackingIndexStep instance) {
        return new ReplaceDataStreamBackingIndexStep(instance.getKey(), instance.getNextStepKey(), instance.getTargetIndexNameSupplier());
    }

    public void testPerformActionThrowsExceptionIfIndexIsNotPartOfDataStream() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(sourceIndexMetadata, true).build())
            .build();

        expectThrows(IllegalStateException.class, () -> createRandomInstance().performAction(sourceIndexMetadata.getIndex(), clusterState));
    }

    public void testPerformActionThrowsExceptionIfIndexIsTheDataStreamWriteIndex() {
        String dataStreamName = randomAlphaOfLength(10);
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String policyName = "test-ilm-policy";
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(sourceIndexMetadata, true)
                    .put(
                        newInstance(
                            dataStreamName,
                            createTimestampField("@timestamp"),
                            org.elasticsearch.core.List.of(sourceIndexMetadata.getIndex())
                        )
                    )
                    .build()
            )
            .build();

        expectThrows(IllegalStateException.class, () -> createRandomInstance().performAction(sourceIndexMetadata.getIndex(), clusterState));
    }

    public void testPerformActionThrowsExceptionIfTargetIndexIsMissing() {
        String dataStreamName = randomAlphaOfLength(10);
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String policyName = "test-ilm-policy";
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
        IndexMetadata writeIndexMetadata = IndexMetadata.builder(writeIndexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        List<Index> backingIndices = org.elasticsearch.core.List.of(sourceIndexMetadata.getIndex(), writeIndexMetadata.getIndex());
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(sourceIndexMetadata, true)
                    .put(writeIndexMetadata, true)
                    .put(newInstance(dataStreamName, createTimestampField("@timestamp"), backingIndices))
                    .build()
            )
            .build();

        expectThrows(IllegalStateException.class, () -> createRandomInstance().performAction(sourceIndexMetadata.getIndex(), clusterState));
    }

    public void testPerformActionIsNoOpIfIndexIsMissing() {
        ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT).build();
        Index missingIndex = new Index("missing", UUID.randomUUID().toString());
        ReplaceDataStreamBackingIndexStep replaceSourceIndexStep = createRandomInstance();
        ClusterState newState = replaceSourceIndexStep.performAction(missingIndex, initialState);
        assertThat(newState, is(initialState));
    }

    public void testPerformAction() {
        String dataStreamName = randomAlphaOfLength(10);
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String policyName = "test-ilm-policy";
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
        IndexMetadata writeIndexMetadata = IndexMetadata.builder(writeIndexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        String indexPrefix = "test-prefix-";
        String targetIndex = indexPrefix + indexName;

        IndexMetadata targetIndexMetadata = IndexMetadata.builder(targetIndex)
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        List<Index> backingIndices = org.elasticsearch.core.List.of(sourceIndexMetadata.getIndex(), writeIndexMetadata.getIndex());
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(sourceIndexMetadata, true)
                    .put(writeIndexMetadata, true)
                    .put(newInstance(dataStreamName, createTimestampField("@timestamp"), backingIndices))
                    .put(targetIndexMetadata, true)
                    .build()
            )
            .build();

        ReplaceDataStreamBackingIndexStep replaceSourceIndexStep = new ReplaceDataStreamBackingIndexStep(
            randomStepKey(),
            randomStepKey(),
            (index, state) -> indexPrefix + index
        );
        ClusterState newState = replaceSourceIndexStep.performAction(sourceIndexMetadata.getIndex(), clusterState);
        DataStream updatedDataStream = newState.metadata().dataStreams().get(dataStreamName);
        assertThat(updatedDataStream.getIndices().size(), is(2));
        assertThat(updatedDataStream.getIndices().get(0), is(targetIndexMetadata.getIndex()));
    }

    /**
     * test IllegalStateException is thrown when original index and write index name are the same
     */
    public void testPerformActionSameOriginalTargetError() {
        String dataStreamName = randomAlphaOfLength(10);
        String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
        String indexName = writeIndexName;
        String policyName = "test-ilm-policy";
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        IndexMetadata writeIndexMetadata = IndexMetadata.builder(writeIndexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        String indexPrefix = "test-prefix-";
        String targetIndex = indexPrefix + indexName;

        IndexMetadata targetIndexMetadata = IndexMetadata.builder(targetIndex)
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        List<Index> backingIndices = org.elasticsearch.core.List.of(writeIndexMetadata.getIndex());
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(sourceIndexMetadata, true)
                    .put(writeIndexMetadata, true)
                    .put(newInstance(dataStreamName, createTimestampField("@timestamp"), backingIndices))
                    .put(targetIndexMetadata, true)
                    .build()
            )
            .build();

        ReplaceDataStreamBackingIndexStep replaceSourceIndexStep = new ReplaceDataStreamBackingIndexStep(
            randomStepKey(),
            randomStepKey(),
            (index, state) -> indexPrefix + index
        );
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> replaceSourceIndexStep.performAction(sourceIndexMetadata.getIndex(), clusterState)
        );
        assertEquals(
            "index ["
                + writeIndexName
                + "] is the write index for data stream ["
                + dataStreamName
                + "], pausing ILM execution of lifecycle [test-ilm-policy] until this index is no longer the write index for the data "
                + "stream via manual or automated rollover",
            ex.getMessage()
        );
    }
}
