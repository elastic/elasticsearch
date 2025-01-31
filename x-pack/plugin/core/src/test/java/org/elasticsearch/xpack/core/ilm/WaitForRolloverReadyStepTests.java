/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxPrimaryShardDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxPrimaryShardSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.MinAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MinDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MinPrimaryShardDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MinPrimaryShardSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.MinSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContentObject;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.SelectorResolver.SELECTOR_SEPARATOR;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class WaitForRolloverReadyStepTests extends AbstractStepTestCase<WaitForRolloverReadyStep> {

    @Override
    protected WaitForRolloverReadyStep createRandomInstance() {
        Step.StepKey stepKey = randomStepKey();
        Step.StepKey nextStepKey = randomStepKey();
        ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue maxSize = randomBoolean() ? null : ByteSizeValue.of(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
        ByteSizeUnit maxPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue maxPrimaryShardSize = randomBoolean()
            ? null
            : ByteSizeValue.of(randomNonNegativeLong() / maxPrimaryShardSizeUnit.toBytes(1), maxPrimaryShardSizeUnit);
        Long maxDocs = randomBoolean() ? null : randomNonNegativeLong();
        TimeValue maxAge = (maxDocs == null && maxSize == null || randomBoolean()) ? randomPositiveTimeValue() : null;
        Long maxPrimaryShardDocs = randomBoolean() ? null : randomNonNegativeLong();
        ByteSizeUnit minSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue minSize = randomBoolean() ? null : ByteSizeValue.of(randomNonNegativeLong() / minSizeUnit.toBytes(1), minSizeUnit);
        ByteSizeUnit minPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue minPrimaryShardSize = randomBoolean()
            ? null
            : ByteSizeValue.of(randomNonNegativeLong() / minPrimaryShardSizeUnit.toBytes(1), minPrimaryShardSizeUnit);
        Long minDocs = randomBoolean() ? null : randomNonNegativeLong();
        TimeValue minAge = (minDocs == null || randomBoolean()) ? randomPositiveTimeValue() : null;
        Long minPrimaryShardDocs = randomBoolean() ? null : randomNonNegativeLong();
        return new WaitForRolloverReadyStep(
            stepKey,
            nextStepKey,
            client,
            maxSize,
            maxPrimaryShardSize,
            maxAge,
            maxDocs,
            maxPrimaryShardDocs,
            minSize,
            minPrimaryShardSize,
            minAge,
            minDocs,
            minPrimaryShardDocs
        );
    }

    @Override
    protected WaitForRolloverReadyStep mutateInstance(WaitForRolloverReadyStep instance) {
        Step.StepKey key = instance.getKey();
        RolloverConditions configuration = instance.getConditions();
        Step.StepKey nextKey = instance.getNextStepKey();
        ByteSizeValue maxSize = configuration.getMaxSize();
        ByteSizeValue maxPrimaryShardSize = configuration.getMaxPrimaryShardSize();
        TimeValue maxAge = configuration.getMaxAge();
        Long maxDocs = configuration.getMaxDocs();
        Long maxPrimaryShardDocs = configuration.getMaxPrimaryShardDocs();
        ByteSizeValue minSize = configuration.getMinSize();
        ByteSizeValue minPrimaryShardSize = configuration.getMinPrimaryShardSize();
        TimeValue minAge = configuration.getMinAge();
        Long minDocs = configuration.getMinDocs();
        Long minPrimaryShardDocs = configuration.getMinPrimaryShardDocs();

        switch (between(0, 11)) {
            case 0 -> key = new Step.StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new Step.StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            case 2 -> maxSize = randomValueOtherThan(maxSize, () -> {
                ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
                return ByteSizeValue.of(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
            });
            case 3 -> maxPrimaryShardSize = randomValueOtherThan(maxPrimaryShardSize, () -> {
                ByteSizeUnit maxPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
                return ByteSizeValue.of(randomNonNegativeLong() / maxPrimaryShardSizeUnit.toBytes(1), maxPrimaryShardSizeUnit);
            });
            case 4 -> maxAge = randomValueOtherThan(maxAge, () -> randomPositiveTimeValue());
            case 5 -> maxDocs = randomValueOtherThan(maxDocs, ESTestCase::randomNonNegativeLong);
            case 6 -> maxPrimaryShardDocs = randomValueOtherThan(maxPrimaryShardDocs, ESTestCase::randomNonNegativeLong);
            case 7 -> minSize = randomValueOtherThan(minSize, () -> {
                ByteSizeUnit minSizeUnit = randomFrom(ByteSizeUnit.values());
                return ByteSizeValue.of(randomNonNegativeLong() / minSizeUnit.toBytes(1), minSizeUnit);
            });
            case 8 -> minPrimaryShardSize = randomValueOtherThan(minPrimaryShardSize, () -> {
                ByteSizeUnit minPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
                return ByteSizeValue.of(randomNonNegativeLong() / minPrimaryShardSizeUnit.toBytes(1), minPrimaryShardSizeUnit);
            });
            case 9 -> minAge = randomValueOtherThan(minAge, () -> randomPositiveTimeValue());
            case 10 -> minDocs = randomValueOtherThan(minDocs, ESTestCase::randomNonNegativeLong);
            case 11 -> minPrimaryShardDocs = randomValueOtherThan(minPrimaryShardDocs, ESTestCase::randomNonNegativeLong);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new WaitForRolloverReadyStep(
            key,
            nextKey,
            instance.getClient(),
            maxSize,
            maxPrimaryShardSize,
            maxAge,
            maxDocs,
            maxPrimaryShardDocs,
            minSize,
            minPrimaryShardSize,
            minAge,
            minDocs,
            minPrimaryShardDocs
        );
    }

    @Override
    protected WaitForRolloverReadyStep copyInstance(WaitForRolloverReadyStep instance) {
        return new WaitForRolloverReadyStep(instance.getKey(), instance.getNextStepKey(), instance.getClient(), instance.getConditions());
    }

    private static void assertRolloverIndexRequest(RolloverRequest request, String rolloverTarget, Set<Condition<?>> expectedConditions) {
        assertNotNull(request);
        assertEquals(1, request.indices().length);
        assertEquals(rolloverTarget, request.indices()[0]);
        assertEquals(rolloverTarget, request.getRolloverTarget());
        assertTrue(request.isDryRun());
        assertEquals(expectedConditions.size(), request.getConditions().getConditions().size());
        Set<Object> expectedConditionValues = expectedConditions.stream().map(Condition::value).collect(Collectors.toSet());
        Set<Object> actualConditionValues = request.getConditionValues().stream().map(Condition::value).collect(Collectors.toSet());
        assertEquals(expectedConditionValues, actualConditionValues);
    }

    private static Set<Condition<?>> getExpectedConditions(WaitForRolloverReadyStep step, boolean maybeAddMinDocs) {
        Set<Condition<?>> expectedConditions = new HashSet<>();
        RolloverConditions conditions = step.getConditions();
        if (conditions.getMaxSize() != null) {
            expectedConditions.add(new MaxSizeCondition(conditions.getMaxSize()));
        }
        if (conditions.getMaxPrimaryShardSize() != null) {
            expectedConditions.add(new MaxPrimaryShardSizeCondition(conditions.getMaxPrimaryShardSize()));
        }
        if (conditions.getMaxAge() != null) {
            expectedConditions.add(new MaxAgeCondition(conditions.getMaxAge()));
        }
        if (conditions.getMaxDocs() != null) {
            expectedConditions.add(new MaxDocsCondition(conditions.getMaxDocs()));
        }
        long maxPrimaryShardDocs;
        if (conditions.getMaxPrimaryShardDocs() != null) {
            maxPrimaryShardDocs = conditions.getMaxPrimaryShardDocs();
            if (maxPrimaryShardDocs > WaitForRolloverReadyStep.MAX_PRIMARY_SHARD_DOCS) {
                maxPrimaryShardDocs = WaitForRolloverReadyStep.MAX_PRIMARY_SHARD_DOCS;
            }
        } else {
            maxPrimaryShardDocs = WaitForRolloverReadyStep.MAX_PRIMARY_SHARD_DOCS;
        }
        expectedConditions.add(new MaxPrimaryShardDocsCondition(maxPrimaryShardDocs));
        if (conditions.getMinSize() != null) {
            expectedConditions.add(new MinSizeCondition(conditions.getMinSize()));
        }
        if (conditions.getMinPrimaryShardSize() != null) {
            expectedConditions.add(new MinPrimaryShardSizeCondition(conditions.getMinPrimaryShardSize()));
        }
        if (conditions.getMinAge() != null) {
            expectedConditions.add(new MinAgeCondition(conditions.getMinAge()));
        }
        if (conditions.getMinDocs() != null) {
            expectedConditions.add(new MinDocsCondition(conditions.getMinDocs()));
        }
        if (conditions.getMinPrimaryShardDocs() != null) {
            expectedConditions.add(new MinPrimaryShardDocsCondition(conditions.getMinPrimaryShardDocs()));
        }

        // if no minimum document condition was specified, then a default min_docs: 1 condition will be injected (if desired)
        if (maybeAddMinDocs && conditions.getMinDocs() == null && conditions.getMinPrimaryShardDocs() == null) {
            expectedConditions.add(new MinDocsCondition(1L));
        }

        return expectedConditions;
    }

    public void testEvaluateCondition() {
        String alias = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putAlias(AliasMetadata.builder(alias).writeIndex(randomFrom(true, null)))
            .settings(settings(IndexVersion.current()).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        WaitForRolloverReadyStep step = createRandomInstance();

        mockRolloverIndexCall(alias, step, true);

        SetOnce<Boolean> conditionsMet = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                conditionsMet.set(complete);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        }, MASTER_TIMEOUT);

        assertEquals(true, conditionsMet.get());

        verify(client, Mockito.only()).admin();
        verify(adminClient, Mockito.only()).indices();
        verify(indicesClient, Mockito.only()).rolloverIndex(Mockito.any(), Mockito.any());
    }

    public void testEvaluateConditionOnDataStreamTarget() {
        String dataStreamName = "test-datastream";
        long ts = System.currentTimeMillis();
        boolean failureStoreIndex = randomBoolean();
        IndexMetadata indexMetadata = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1, ts))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata failureStoreMetadata = IndexMetadata.builder(DataStream.getDefaultFailureStoreName(dataStreamName, 1, ts))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        WaitForRolloverReadyStep step = createRandomInstance();

        mockRolloverIndexCall(
            failureStoreIndex ? dataStreamName + SELECTOR_SEPARATOR + IndexComponentSelector.FAILURES.getKey() : dataStreamName,
            step,
            true
        );

        SetOnce<Boolean> conditionsMet = new SetOnce<>();
        Metadata metadata = Metadata.builder()
            .put(indexMetadata, true)
            .put(failureStoreMetadata, true)
            .put(
                DataStreamTestHelper.newInstance(
                    dataStreamName,
                    List.of(indexMetadata.getIndex()),
                    List.of(failureStoreMetadata.getIndex())
                )
            )
            .build();
        IndexMetadata indexToOperateOn = failureStoreIndex ? failureStoreMetadata : indexMetadata;
        step.evaluateCondition(metadata, indexToOperateOn.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                conditionsMet.set(complete);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        }, MASTER_TIMEOUT);

        assertEquals(true, conditionsMet.get());

        verify(client, Mockito.only()).admin();
        verify(adminClient, Mockito.only()).indices();

        ArgumentCaptor<RolloverRequest> requestCaptor = ArgumentCaptor.forClass(RolloverRequest.class);
        verify(indicesClient, Mockito.only()).rolloverIndex(requestCaptor.capture(), Mockito.any());

        RolloverRequest request = requestCaptor.getValue();
        if (failureStoreIndex == false) {
            assertThat(request.getRolloverTarget(), equalTo(dataStreamName));
        } else {
            assertThat(
                request.getRolloverTarget(),
                equalTo(dataStreamName + SELECTOR_SEPARATOR + IndexComponentSelector.FAILURES.getKey())
            );
        }
    }

    public void testSkipRolloverIfDataStreamIsAlreadyRolledOver() {
        String dataStreamName = "test-datastream";
        long ts = System.currentTimeMillis();
        boolean failureStoreIndex = randomBoolean();
        IndexMetadata firstGenerationIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1, ts))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        IndexMetadata writeIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 2, ts))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        IndexMetadata firstGenerationFailureIndex = IndexMetadata.builder(DataStream.getDefaultFailureStoreName(dataStreamName, 1, ts))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        IndexMetadata writeFailureIndex = IndexMetadata.builder(DataStream.getDefaultFailureStoreName(dataStreamName, 2, ts))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        WaitForRolloverReadyStep step = createRandomInstance();

        SetOnce<Boolean> conditionsMet = new SetOnce<>();
        Metadata metadata = Metadata.builder()
            .put(firstGenerationIndex, true)
            .put(writeIndex, true)
            .put(firstGenerationFailureIndex, true)
            .put(writeFailureIndex, true)
            .put(
                DataStreamTestHelper.newInstance(
                    dataStreamName,
                    List.of(firstGenerationIndex.getIndex(), writeIndex.getIndex()),
                    List.of(firstGenerationFailureIndex.getIndex(), writeFailureIndex.getIndex())
                )
            )
            .build();
        IndexMetadata indexToOperateOn = failureStoreIndex ? firstGenerationFailureIndex : firstGenerationIndex;
        step.evaluateCondition(metadata, indexToOperateOn.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                conditionsMet.set(complete);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        }, MASTER_TIMEOUT);

        assertEquals(true, conditionsMet.get());

        verifyNoMoreInteractions(client);
        verifyNoMoreInteractions(adminClient);
        verifyNoMoreInteractions(indicesClient);
    }

    private void mockRolloverIndexCall(String rolloverTarget, WaitForRolloverReadyStep step, boolean conditionResult) {
        Mockito.doAnswer(invocation -> {
            RolloverRequest request = (RolloverRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArguments()[1];
            Set<Condition<?>> expectedConditions = getExpectedConditions(step, true);
            assertRolloverIndexRequest(request, rolloverTarget, expectedConditions);
            Map<String, Boolean> conditionResults = expectedConditions.stream()
                .collect(Collectors.toMap(Condition::toString, condition -> conditionResult));
            listener.onResponse(new RolloverResponse(null, null, conditionResults, request.isDryRun(), false, false, false, false));
            return null;
        }).when(indicesClient).rolloverIndex(Mockito.any(), Mockito.any());
    }

    public void testEvaluateDoesntTriggerRolloverForIndexManuallyRolledOnLifecycleRolloverAlias() {
        String rolloverAlias = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putAlias(AliasMetadata.builder(rolloverAlias))
            .settings(settings(IndexVersion.current()).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, rolloverAlias))
            .putRolloverInfo(
                new RolloverInfo(rolloverAlias, List.of(new MaxSizeCondition(ByteSizeValue.ofBytes(2L))), System.currentTimeMillis())
            )
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        WaitForRolloverReadyStep step = createRandomInstance();

        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                assertThat(complete, is(true));
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        }, MASTER_TIMEOUT);

        verify(indicesClient, Mockito.never()).rolloverIndex(Mockito.any(), Mockito.any());
    }

    public void testEvaluateTriggersRolloverForIndexManuallyRolledOnDifferentAlias() {
        String rolloverAlias = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putAlias(AliasMetadata.builder(rolloverAlias))
            .settings(settings(IndexVersion.current()).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, rolloverAlias))
            .putRolloverInfo(
                new RolloverInfo(
                    randomAlphaOfLength(5),
                    List.of(new MaxSizeCondition(ByteSizeValue.ofBytes(2L))),
                    System.currentTimeMillis()
                )
            )
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        WaitForRolloverReadyStep step = createRandomInstance();

        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                assertThat(complete, is(true));
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        }, MASTER_TIMEOUT);

        verify(indicesClient, Mockito.only()).rolloverIndex(Mockito.any(), Mockito.any());
    }

    public void testPerformActionWriteIndexIsFalse() {
        String alias = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putAlias(AliasMetadata.builder(alias).writeIndex(false))
            .settings(settings(IndexVersion.current()).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        WaitForRolloverReadyStep step = createRandomInstance();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                fail("expecting failure as the write index must be set to true or null");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(
                    e.getMessage(),
                    is(
                        String.format(
                            Locale.ROOT,
                            "index [%s] is not the write index for alias [%s]",
                            indexMetadata.getIndex().getName(),
                            alias
                        )
                    )
                );
            }
        }, MASTER_TIMEOUT);

        verify(client, times(0)).admin();
    }

    public void testPerformActionWithIndexingComplete() {
        String alias = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putAlias(AliasMetadata.builder(alias).writeIndex(randomFrom(false, null)))
            .settings(
                settings(IndexVersion.current()).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
                    .put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, true)
            )
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        WaitForRolloverReadyStep step = createRandomInstance();

        SetOnce<Boolean> conditionsMet = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                conditionsMet.set(complete);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        }, MASTER_TIMEOUT);

        assertEquals(true, conditionsMet.get());
    }

    public void testPerformActionWithIndexingCompleteStillWriteIndex() {
        String alias = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putAlias(AliasMetadata.builder(alias).writeIndex(true))
            .settings(
                settings(IndexVersion.current()).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
                    .put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, true)
            )
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        WaitForRolloverReadyStep step = createRandomInstance();

        SetOnce<Boolean> correctFailureCalled = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                throw new AssertionError("Should have failed with indexing_complete but index is not write index");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof IllegalStateException);
                correctFailureCalled.set(true);
            }
        }, MASTER_TIMEOUT);

        assertEquals(true, correctFailureCalled.get());
    }

    public void testPerformActionNotComplete() {
        String alias = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putAlias(AliasMetadata.builder(alias))
            .settings(settings(IndexVersion.current()).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        WaitForRolloverReadyStep step = createRandomInstance();

        mockRolloverIndexCall(alias, step, false);

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                actionCompleted.set(complete);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        }, MASTER_TIMEOUT);

        assertEquals(false, actionCompleted.get());

        verify(client, Mockito.only()).admin();
        verify(adminClient, Mockito.only()).indices();
        verify(indicesClient, Mockito.only()).rolloverIndex(Mockito.any(), Mockito.any());
    }

    public void testPerformActionFailure() {
        String alias = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putAlias(AliasMetadata.builder(alias))
            .settings(settings(IndexVersion.current()).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        Exception exception = new RuntimeException();
        WaitForRolloverReadyStep step = createRandomInstance();

        Mockito.doAnswer(invocation -> {
            RolloverRequest request = (RolloverRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArguments()[1];
            Set<Condition<?>> expectedConditions = getExpectedConditions(step, true);
            assertRolloverIndexRequest(request, alias, expectedConditions);
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).rolloverIndex(Mockito.any(), Mockito.any());

        SetOnce<Boolean> exceptionThrown = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                assertSame(exception, e);
                exceptionThrown.set(true);
            }
        }, MASTER_TIMEOUT);

        assertEquals(true, exceptionThrown.get());

        verify(client, Mockito.only()).admin();
        verify(adminClient, Mockito.only()).indices();
        verify(indicesClient, Mockito.only()).rolloverIndex(Mockito.any(), Mockito.any());
    }

    public void testPerformActionInvalidNullOrEmptyAlias() {
        String alias = randomBoolean() ? "" : null;
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(IndexVersion.current()).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        WaitForRolloverReadyStep step = createRandomInstance();

        SetOnce<Exception> exceptionThrown = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionThrown.set(e);
            }
        }, MASTER_TIMEOUT);
        assertThat(exceptionThrown.get().getClass(), equalTo(IllegalArgumentException.class));
        assertThat(
            exceptionThrown.get().getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "setting [%s] for index [%s] is empty or not defined",
                    RolloverAction.LIFECYCLE_ROLLOVER_ALIAS,
                    indexMetadata.getIndex().getName()
                )
            )
        );
    }

    public void testPerformActionAliasDoesNotPointToIndex() {
        String alias = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(IndexVersion.current()).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        WaitForRolloverReadyStep step = createRandomInstance();

        SetOnce<Exception> exceptionThrown = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionThrown.set(e);
            }
        }, MASTER_TIMEOUT);
        assertThat(exceptionThrown.get().getClass(), equalTo(IllegalArgumentException.class));
        assertThat(
            exceptionThrown.get().getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "%s [%s] does not point to index [%s]",
                    RolloverAction.LIFECYCLE_ROLLOVER_ALIAS,
                    alias,
                    indexMetadata.getIndex().getName()
                )
            )
        );
    }

    public void testCreateRolloverRequestRolloverOnlyIfHasDocuments() {
        boolean rolloverOnlyIfHasDocuments = randomBoolean();

        WaitForRolloverReadyStep step = createRandomInstance();
        String rolloverTarget = randomAlphaOfLength(5);
        TimeValue masterTimeout = randomPositiveTimeValue();

        RolloverRequest request = step.createRolloverRequest(rolloverTarget, masterTimeout, rolloverOnlyIfHasDocuments, false);

        assertThat(request.getRolloverTarget(), is(rolloverTarget));
        assertThat(request.masterNodeTimeout(), is(masterTimeout));
        assertThat(request.isDryRun(), is(true)); // it's always a dry_run

        Set<Condition<?>> expectedConditions = getExpectedConditions(step, rolloverOnlyIfHasDocuments);
        assertEquals(expectedConditions.size(), request.getConditions().getConditions().size());
        Set<Object> expectedConditionValues = expectedConditions.stream().map(Condition::value).collect(Collectors.toSet());
        Set<Object> actualConditionValues = request.getConditions()
            .getConditions()
            .values()
            .stream()
            .map(Condition::value)
            .collect(Collectors.toSet());
        assertEquals(expectedConditionValues, actualConditionValues);
    }

    public void testCreateRolloverRequestRolloverBeyondMaximumPrimaryShardDocCount() {
        WaitForRolloverReadyStep step = createRandomInstance();
        String rolloverTarget = randomAlphaOfLength(5);
        TimeValue masterTimeout = randomPositiveTimeValue();
        var c = step.getConditions();
        // If beyond MAX_PRIMARY_SHARD_DOCS_FOR_TSDB then expected is always MAX_PRIMARY_SHARD_DOCS_FOR_TSDB
        step = new WaitForRolloverReadyStep(
            step.getKey(),
            step.getNextStepKey(),
            step.getClient(),
            c.getMaxSize(),
            c.getMaxPrimaryShardSize(),
            c.getMaxAge(),
            c.getMaxDocs(),
            randomLongBetween(WaitForRolloverReadyStep.MAX_PRIMARY_SHARD_DOCS, Long.MAX_VALUE),
            c.getMinSize(),
            c.getMinPrimaryShardSize(),
            c.getMinAge(),
            c.getMinDocs(),
            c.getMinPrimaryShardDocs()
        );
        RolloverRequest request = step.createRolloverRequest(rolloverTarget, masterTimeout, true, false);
        assertThat(request.getRolloverTarget(), is(rolloverTarget));
        assertThat(request.masterNodeTimeout(), is(masterTimeout));
        assertThat(request.isDryRun(), is(true)); // it's always a dry_run
        assertThat(request.getConditions().getMaxPrimaryShardDocs(), equalTo(WaitForRolloverReadyStep.MAX_PRIMARY_SHARD_DOCS));
        // If null then expected is always MAX_PRIMARY_SHARD_DOCS_FOR_TSDB
        step = new WaitForRolloverReadyStep(
            step.getKey(),
            step.getNextStepKey(),
            step.getClient(),
            c.getMaxSize(),
            c.getMaxPrimaryShardSize(),
            c.getMaxAge(),
            c.getMaxDocs(),
            null,
            c.getMinSize(),
            c.getMinPrimaryShardSize(),
            c.getMinAge(),
            c.getMinDocs(),
            c.getMinPrimaryShardDocs()
        );
        request = step.createRolloverRequest(rolloverTarget, masterTimeout, true, false);
        assertThat(request.getRolloverTarget(), is(rolloverTarget));
        assertThat(request.masterNodeTimeout(), is(masterTimeout));
        assertThat(request.isDryRun(), is(true)); // it's always a dry_run
        assertThat(request.getConditions().getMaxPrimaryShardDocs(), equalTo(WaitForRolloverReadyStep.MAX_PRIMARY_SHARD_DOCS));
        // If less then WaitForRolloverReadyStep.MAX_PRIMARY_SHARD_DOCS_FOR_TSDB then expected is what has been defined
        long maxPrimaryShardDocCount;
        step = new WaitForRolloverReadyStep(
            step.getKey(),
            step.getNextStepKey(),
            step.getClient(),
            c.getMaxSize(),
            c.getMaxPrimaryShardSize(),
            c.getMaxAge(),
            c.getMaxDocs(),
            maxPrimaryShardDocCount = randomLongBetween(1, WaitForRolloverReadyStep.MAX_PRIMARY_SHARD_DOCS - 1),
            c.getMinSize(),
            c.getMinPrimaryShardSize(),
            c.getMinAge(),
            c.getMinDocs(),
            c.getMinPrimaryShardDocs()
        );
        request = step.createRolloverRequest(rolloverTarget, masterTimeout, true, false);
        assertThat(request.getRolloverTarget(), is(rolloverTarget));
        assertThat(request.masterNodeTimeout(), is(masterTimeout));
        assertThat(request.isDryRun(), is(true)); // it's always a dry_run
        assertThat(request.getConditions().getMaxPrimaryShardDocs(), equalTo(maxPrimaryShardDocCount));
    }
}
