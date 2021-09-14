/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;

import java.util.List;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createTimestampField;
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
            case 0:
                key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
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
        IndexMetadata indexMetadata =
            IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        ClusterState clusterState = ClusterState.builder(emptyClusterState()).metadata(
            Metadata.builder().put(indexMetadata, true).build()
        ).build();

        ClusterStateWaitStep.Result result = createRandomInstance().isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(true));
        assertThat(result.getInfomationContext(), is(nullValue()));
    }

    public void testStepIncompleteIfIndexIsTheDataStreamWriteIndex() {
        String dataStreamName = randomAlphaOfLength(10);
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String policyName = "test-ilm-policy";
        IndexMetadata indexMetadata =
            IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        ClusterState clusterState = ClusterState.builder(emptyClusterState()).metadata(
            Metadata.builder().put(indexMetadata, true).put(new DataStream(dataStreamName,
                createTimestampField("@timestamp"), List.of(indexMetadata.getIndex()))).build()
        ).build();

        ClusterStateWaitStep.Result result = createRandomInstance().isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        CheckNotDataStreamWriteIndexStep.Info info = (CheckNotDataStreamWriteIndexStep.Info) result.getInfomationContext();
        assertThat(info.getMessage(), is("index [" + indexName + "] is the write index for data stream [" + dataStreamName + "], " +
            "pausing ILM execution of lifecycle [" + policyName + "] until this index is no longer the write index for the data stream " +
            "via manual or automated rollover"));
    }

    public void testStepCompleteIfPartOfDataStreamButNotWriteIndex() {
        String dataStreamName = randomAlphaOfLength(10);
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String policyName = "test-ilm-policy";
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5))
            .build();

        String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
        IndexMetadata writeIndexMetadata = IndexMetadata.builder(writeIndexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5))
            .build();

        List<Index> backingIndices = List.of(indexMetadata.getIndex(), writeIndexMetadata.getIndex());
        ClusterState clusterState = ClusterState.builder(emptyClusterState()).metadata(
            Metadata.builder()
                .put(indexMetadata, true)
                .put(writeIndexMetadata, true)
                .put(new DataStream(dataStreamName, createTimestampField("@timestamp"), backingIndices))
                .build()
        ).build();

        ClusterStateWaitStep.Result result = createRandomInstance().isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(true));
        assertThat(result.getInfomationContext(), is(nullValue()));
    }
}
