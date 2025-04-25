/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;

import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;

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
            case 0 -> key = new Step.StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new Step.StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            case 2 -> indexNameSupplier = (index, state) -> randomAlphaOfLengthBetween(11, 15) + index;
            default -> throw new AssertionError("Illegal randomisation branch");
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
        IndexMetadata.Builder sourceIndexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5));

        final IndexMetadata sourceIndexMetadata = sourceIndexMetadataBuilder.build();
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(sourceIndexMetadata, false).build())
            .build();

        expectThrows(IllegalStateException.class, () -> createRandomInstance().performAction(sourceIndexMetadata.getIndex(), clusterState));
    }

    public void testPerformActionThrowsExceptionIfIndexIsTheDataStreamWriteIndex() {
        String dataStreamName = randomAlphaOfLength(10);
        long ts = System.currentTimeMillis();
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1, ts);
        String failureIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, ts);
        String policyName = "test-ilm-policy";
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata failureSourceIndexMetadata = IndexMetadata.builder(failureIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(sourceIndexMetadata, true)
                    .put(failureSourceIndexMetadata, true)
                    .put(
                        newInstance(dataStreamName, List.of(sourceIndexMetadata.getIndex()), List.of(failureSourceIndexMetadata.getIndex()))
                    )
                    .build()
            )
            .build();

        boolean useFailureStore = randomBoolean();
        IndexMetadata indexToOperateOn = useFailureStore ? failureSourceIndexMetadata : sourceIndexMetadata;
        expectThrows(IllegalStateException.class, () -> createRandomInstance().performAction(indexToOperateOn.getIndex(), clusterState));
    }

    public void testPerformActionThrowsExceptionIfTargetIndexIsMissing() {
        String dataStreamName = randomAlphaOfLength(10);
        long ts = System.currentTimeMillis();
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1, ts);
        String failureIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, ts);
        String policyName = "test-ilm-policy";
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata failureSourceIndexMetadata = IndexMetadata.builder(failureIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2, ts);
        String failureWriteIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 2, ts);
        IndexMetadata writeIndexMetadata = IndexMetadata.builder(writeIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata failureWriteIndexMetadata = IndexMetadata.builder(failureWriteIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        List<Index> backingIndices = List.of(sourceIndexMetadata.getIndex(), writeIndexMetadata.getIndex());
        List<Index> failureIndices = List.of(failureSourceIndexMetadata.getIndex(), failureWriteIndexMetadata.getIndex());
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(sourceIndexMetadata, true)
                    .put(writeIndexMetadata, true)
                    .put(failureSourceIndexMetadata, true)
                    .put(failureWriteIndexMetadata, true)
                    .put(newInstance(dataStreamName, backingIndices, failureIndices))
                    .build()
            )
            .build();

        boolean useFailureStore = randomBoolean();
        IndexMetadata indexToOperateOn = useFailureStore ? failureSourceIndexMetadata : sourceIndexMetadata;
        expectThrows(IllegalStateException.class, () -> createRandomInstance().performAction(indexToOperateOn.getIndex(), clusterState));
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
        long ts = System.currentTimeMillis();
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1, ts);
        String failureIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, ts);
        String policyName = "test-ilm-policy";
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata failureSourceIndexMetadata = IndexMetadata.builder(failureIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2, ts);
        String failureWriteIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 2, ts);
        IndexMetadata writeIndexMetadata = IndexMetadata.builder(writeIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata failureWriteIndexMetadata = IndexMetadata.builder(failureWriteIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        boolean useFailureStore = randomBoolean();
        String indexNameToUse = useFailureStore ? failureIndexName : indexName;

        String indexPrefix = "test-prefix-";
        String targetIndex = indexPrefix + indexNameToUse;

        IndexMetadata targetIndexMetadata = IndexMetadata.builder(targetIndex)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        List<Index> backingIndices = List.of(sourceIndexMetadata.getIndex(), writeIndexMetadata.getIndex());
        List<Index> failureIndices = List.of(failureSourceIndexMetadata.getIndex(), failureWriteIndexMetadata.getIndex());
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(sourceIndexMetadata, true)
                    .put(writeIndexMetadata, true)
                    .put(failureSourceIndexMetadata, true)
                    .put(failureWriteIndexMetadata, true)
                    .put(newInstance(dataStreamName, backingIndices, failureIndices))
                    .put(targetIndexMetadata, true)
                    .build()
            )
            .build();

        ReplaceDataStreamBackingIndexStep replaceSourceIndexStep = new ReplaceDataStreamBackingIndexStep(
            randomStepKey(),
            randomStepKey(),
            (index, state) -> indexPrefix + indexNameToUse
        );
        IndexMetadata indexToOperateOn = useFailureStore ? failureSourceIndexMetadata : sourceIndexMetadata;
        ClusterState newState = replaceSourceIndexStep.performAction(indexToOperateOn.getIndex(), clusterState);
        DataStream updatedDataStream = newState.metadata().getProject().dataStreams().get(dataStreamName);
        DataStream.DataStreamIndices resultIndices = updatedDataStream.getDataStreamIndices(useFailureStore);
        assertThat(resultIndices.getIndices().size(), is(2));
        assertThat(resultIndices.getIndices().get(0), is(targetIndexMetadata.getIndex()));
    }

    /**
     * test IllegalStateException is thrown when original index and write index name are the same
     */
    public void testPerformActionSameOriginalTargetError() {
        String dataStreamName = randomAlphaOfLength(10);
        long ts = System.currentTimeMillis();
        String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2, ts);
        String failureWriteIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 2, ts);
        String indexName = writeIndexName;
        String failureIndexName = failureWriteIndexName;
        String policyName = "test-ilm-policy";
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata failureSourceIndexMetadata = IndexMetadata.builder(failureIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        IndexMetadata writeIndexMetadata = IndexMetadata.builder(writeIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata failureWriteIndexMetadata = IndexMetadata.builder(failureWriteIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        String indexPrefix = "test-prefix-";
        boolean useFailureStore = randomBoolean();
        String indexNameToUse = useFailureStore ? failureIndexName : indexName;
        String targetIndex = indexPrefix + indexNameToUse;

        IndexMetadata targetIndexMetadata = IndexMetadata.builder(targetIndex)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        List<Index> backingIndices = List.of(writeIndexMetadata.getIndex());
        List<Index> failureIndices = List.of(failureWriteIndexMetadata.getIndex());
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(sourceIndexMetadata, true)
                    .put(writeIndexMetadata, true)
                    .put(failureSourceIndexMetadata, true)
                    .put(failureWriteIndexMetadata, true)
                    .put(newInstance(dataStreamName, backingIndices, failureIndices))
                    .put(targetIndexMetadata, true)
                    .build()
            )
            .build();

        ReplaceDataStreamBackingIndexStep replaceSourceIndexStep = new ReplaceDataStreamBackingIndexStep(
            randomStepKey(),
            randomStepKey(),
            (index, state) -> indexPrefix + index
        );
        IndexMetadata indexToOperateOn = useFailureStore ? failureSourceIndexMetadata : sourceIndexMetadata;
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> replaceSourceIndexStep.performAction(indexToOperateOn.getIndex(), clusterState)
        );
        assertEquals(
            "index ["
                + indexNameToUse
                + "] is the "
                + (useFailureStore ? "failure store " : "")
                + "write index for data stream ["
                + dataStreamName
                + "], pausing ILM execution of lifecycle [test-ilm-policy] until this index is no longer the write index for the data "
                + "stream via manual or automated rollover",
            ex.getMessage()
        );
    }
}
