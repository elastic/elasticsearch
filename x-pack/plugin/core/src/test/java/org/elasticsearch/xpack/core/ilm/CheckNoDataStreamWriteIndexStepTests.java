/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.core.ilm.step.info.SingleMessageFieldInfo;

import java.util.List;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class CheckNoDataStreamWriteIndexStepTests extends AbstractStepTestCase<CheckNotDataStreamWriteIndexStep> {

    @Override
    protected CheckNotDataStreamWriteIndexStep createRandomInstance() {
        return new CheckNotDataStreamWriteIndexStep(randomStepKey(), randomStepKey());
    }

    @Override
    protected CheckNotDataStreamWriteIndexStep mutateInstance(CheckNotDataStreamWriteIndexStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0 -> key = new Step.StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new Step.StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new CheckNotDataStreamWriteIndexStep(key, nextKey);
    }

    @Override
    protected CheckNotDataStreamWriteIndexStep copyInstance(CheckNotDataStreamWriteIndexStep instance) {
        return new CheckNotDataStreamWriteIndexStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testStepCompleteIfIndexIsNotPartOfDataStream() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();

        ClusterStateWaitStep.Result result = createRandomInstance().isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.complete(), is(true));
        assertThat(result.informationContext(), is(nullValue()));
    }

    public void testStepIncompleteIfIndexIsTheDataStreamWriteIndex() {
        String dataStreamName = randomAlphaOfLength(10);
        long ts = System.currentTimeMillis();
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1, ts);
        String failureIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, ts);
        String policyName = "test-ilm-policy";
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata failureIndexMetadata = IndexMetadata.builder(failureIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(indexMetadata, true)
                    .put(failureIndexMetadata, true)
                    .put(newInstance(dataStreamName, List.of(indexMetadata.getIndex()), List.of(failureIndexMetadata.getIndex())))
                    .build()
            )
            .build();

        boolean useFailureStore = randomBoolean();
        IndexMetadata indexToOperateOn = useFailureStore ? failureIndexMetadata : indexMetadata;
        String expectedIndexName = indexToOperateOn.getIndex().getName();
        ClusterStateWaitStep.Result result = createRandomInstance().isConditionMet(indexToOperateOn.getIndex(), clusterState);
        assertThat(result.complete(), is(false));
        SingleMessageFieldInfo info = (SingleMessageFieldInfo) result.informationContext();
        assertThat(
            info.message(),
            is(
                "index ["
                    + expectedIndexName
                    + "] is the "
                    + (useFailureStore ? "failure store " : "")
                    + "write index for data stream ["
                    + dataStreamName
                    + "], "
                    + "pausing ILM execution of lifecycle ["
                    + policyName
                    + "] until this index is no longer the write index for the data stream "
                    + "via manual or automated rollover"
            )
        );
    }

    public void testStepCompleteIfPartOfDataStreamButNotWriteIndex() {
        String dataStreamName = randomAlphaOfLength(10);
        long ts = System.currentTimeMillis();
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1, ts);
        String failureIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, ts);
        String policyName = "test-ilm-policy";
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata failureIndexMetadata = IndexMetadata.builder(failureIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2, ts);
        String failureStoreWriteIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, 2, ts);
        IndexMetadata writeIndexMetadata = IndexMetadata.builder(writeIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata failureStoreWriteIndexMetadata = IndexMetadata.builder(failureStoreWriteIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        List<Index> backingIndices = List.of(indexMetadata.getIndex(), writeIndexMetadata.getIndex());
        List<Index> failureIndices = List.of(failureIndexMetadata.getIndex(), failureStoreWriteIndexMetadata.getIndex());
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(indexMetadata, true)
                    .put(writeIndexMetadata, true)
                    .put(failureIndexMetadata, true)
                    .put(failureStoreWriteIndexMetadata, true)
                    .put(newInstance(dataStreamName, backingIndices, failureIndices))
                    .build()
            )
            .build();

        boolean useFailureStore = randomBoolean();
        IndexMetadata indexToOperateOn = useFailureStore ? failureIndexMetadata : indexMetadata;
        ClusterStateWaitStep.Result result = createRandomInstance().isConditionMet(indexToOperateOn.getIndex(), clusterState);
        assertThat(result.complete(), is(true));
        assertThat(result.informationContext(), is(nullValue()));
    }
}
