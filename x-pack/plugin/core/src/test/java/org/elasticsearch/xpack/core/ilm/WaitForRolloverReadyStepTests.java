/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WaitForRolloverReadyStepTests extends AbstractStepTestCase<WaitForRolloverReadyStep> {

    @Override
    protected WaitForRolloverReadyStep createRandomInstance() {
        Step.StepKey stepKey = randomStepKey();
        Step.StepKey nextStepKey = randomStepKey();
        ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue maxSize = randomBoolean() ? null : new ByteSizeValue(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
        Long maxDocs = randomBoolean() ? null : randomNonNegativeLong();
        TimeValue maxAge = (maxDocs == null && maxSize == null || randomBoolean())
            ? TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test")
            : null;
        return new WaitForRolloverReadyStep(stepKey, nextStepKey, client, maxSize, maxAge, maxDocs);
    }

    @Override
    protected WaitForRolloverReadyStep mutateInstance(WaitForRolloverReadyStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();
        ByteSizeValue maxSize = instance.getMaxSize();
        TimeValue maxAge = instance.getMaxAge();
        Long maxDocs = instance.getMaxDocs();

        switch (between(0, 4)) {
            case 0:
                key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 2:
                maxSize = randomValueOtherThan(maxSize, () -> {
                    ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
                    return new ByteSizeValue(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
                });
                break;
            case 3:
                maxAge = randomValueOtherThan(maxAge, () -> TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test"));
                break;
            case 4:
                maxDocs = randomValueOtherThan(maxDocs, () -> randomNonNegativeLong());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new WaitForRolloverReadyStep(key, nextKey, instance.getClient(), maxSize, maxAge, maxDocs);
    }

    @Override
    protected WaitForRolloverReadyStep copyInstance(WaitForRolloverReadyStep instance) {
        return new WaitForRolloverReadyStep(instance.getKey(), instance.getNextStepKey(), instance.getClient(),
            instance.getMaxSize(), instance.getMaxAge(), instance.getMaxDocs());
    }

    private static void assertRolloverIndexRequest(RolloverRequest request, String rolloverTarget, Set<Condition<?>> expectedConditions) {
        assertNotNull(request);
        assertEquals(1, request.indices().length);
        assertEquals(rolloverTarget, request.indices()[0]);
        assertEquals(rolloverTarget, request.getRolloverTarget());
        assertEquals(expectedConditions.size(), request.getConditions().size());
        assertTrue(request.isDryRun());
        Set<Object> expectedConditionValues = expectedConditions.stream().map(Condition::value).collect(Collectors.toSet());
        Set<Object> actualConditionValues = request.getConditions().values().stream()
            .map(Condition::value).collect(Collectors.toSet());
        assertEquals(expectedConditionValues, actualConditionValues);
    }

    public void testEvaluateCondition() {
        String alias = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putAlias(AliasMetadata.builder(alias).writeIndex(randomFrom(true, null)))
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        WaitForRolloverReadyStep step = createRandomInstance();

        mockRolloverIndexCall(alias, step);

        SetOnce<Boolean> conditionsMet = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject infomationContext) {
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
        IndexMetadata indexMetadata = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        WaitForRolloverReadyStep step = createRandomInstance();

        mockRolloverIndexCall(dataStreamName, step);

        SetOnce<Boolean> conditionsMet = new SetOnce<>();
        Metadata metadata = Metadata.builder().put(indexMetadata, true)
            .put(new DataStream(dataStreamName, createTimestampField("timestamp"),
                List.of(indexMetadata.getIndex()), 1L))
            .build();
        step.evaluateCondition(metadata, indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject infomationContext) {
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

    private void mockRolloverIndexCall(String rolloverTarget, WaitForRolloverReadyStep step) {
        Mockito.doAnswer(invocation -> {
            RolloverRequest request = (RolloverRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArguments()[1];
            Set<Condition<?>> expectedConditions = new HashSet<>();
            if (step.getMaxAge() != null) {
                expectedConditions.add(new MaxAgeCondition(step.getMaxAge()));
            }
            if (step.getMaxSize() != null) {
                expectedConditions.add(new MaxSizeCondition(step.getMaxSize()));
            }
            if (step.getMaxDocs() != null) {
                expectedConditions.add(new MaxDocsCondition(step.getMaxDocs()));
            }
            assertRolloverIndexRequest(request, rolloverTarget, expectedConditions);
            Map<String, Boolean> conditionResults = expectedConditions.stream()
                .collect(Collectors.toMap(Condition::toString, condition -> true));
            listener.onResponse(new RolloverResponse(null, null, conditionResults, request.isDryRun(), false, false, false));
            return null;
        }).when(indicesClient).rolloverIndex(Mockito.any(), Mockito.any());
    }

    public void testEvaluateDoesntTriggerRolloverForIndexManuallyRolledOnLifecycleRolloverAlias() {
        String rolloverAlias = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putAlias(AliasMetadata.builder(rolloverAlias))
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, rolloverAlias))
            .putRolloverInfo(new RolloverInfo(rolloverAlias, Collections.singletonList(new MaxSizeCondition(new ByteSizeValue(2L))),
                System.currentTimeMillis()))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

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
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, rolloverAlias))
            .putRolloverInfo(new RolloverInfo(randomAlphaOfLength(5),
                Collections.singletonList(new MaxSizeCondition(new ByteSizeValue(2L))),
                System.currentTimeMillis())
            )
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

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
            .settings(settings(Version.CURRENT)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        WaitForRolloverReadyStep step = createRandomInstance();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject infomationContext) {
                fail("expecting failure as the write index must be set to true or null");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e.getMessage(), is(String.format(Locale.ROOT, "index [%s] is not the write index for alias [%s]",
                    indexMetadata.getIndex().getName(), alias)));
            }
        }, MASTER_TIMEOUT);

        verify(client, times(0)).admin();
    }

    public void testPerformActionWithIndexingComplete() {
        String alias = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putAlias(AliasMetadata.builder(alias).writeIndex(randomFrom(false, null)))
            .settings(settings(Version.CURRENT)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
                .put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, true))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        WaitForRolloverReadyStep step = createRandomInstance();

        SetOnce<Boolean> conditionsMet = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject infomationContext) {
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
            .settings(settings(Version.CURRENT)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
                .put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, true))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        WaitForRolloverReadyStep step = createRandomInstance();

        SetOnce<Boolean> correctFailureCalled = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject infomationContext) {
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
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        WaitForRolloverReadyStep step = createRandomInstance();

        Mockito.doAnswer(invocation -> {
            RolloverRequest request = (RolloverRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArguments()[1];
            Set<Condition<?>> expectedConditions = new HashSet<>();
            if (step.getMaxAge() != null) {
                expectedConditions.add(new MaxAgeCondition(step.getMaxAge()));
            }
            if (step.getMaxSize() != null) {
                expectedConditions.add(new MaxSizeCondition(step.getMaxSize()));
            }
            if (step.getMaxDocs() != null) {
                expectedConditions.add(new MaxDocsCondition(step.getMaxDocs()));
            }
            assertRolloverIndexRequest(request, alias, expectedConditions);
            Map<String, Boolean> conditionResults = expectedConditions.stream()
                .collect(Collectors.toMap(Condition::toString, condition -> false));
            listener.onResponse(new RolloverResponse(null, null, conditionResults, request.isDryRun(), false, false, false));
            return null;
        }).when(indicesClient).rolloverIndex(Mockito.any(), Mockito.any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject infomationContext) {
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
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Exception exception = new RuntimeException();
        WaitForRolloverReadyStep step = createRandomInstance();

        Mockito.doAnswer(invocation -> {
            RolloverRequest request = (RolloverRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArguments()[1];
            Set<Condition<?>> expectedConditions = new HashSet<>();
            if (step.getMaxAge() != null) {
                expectedConditions.add(new MaxAgeCondition(step.getMaxAge()));
            }
            if (step.getMaxSize() != null) {
                expectedConditions.add(new MaxSizeCondition(step.getMaxSize()));
            }
            if (step.getMaxDocs() != null) {
                expectedConditions.add(new MaxDocsCondition(step.getMaxDocs()));
            }
            assertRolloverIndexRequest(request, alias, expectedConditions);
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).rolloverIndex(Mockito.any(), Mockito.any());

        SetOnce<Boolean> exceptionThrown = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

            @Override
            public void onResponse(boolean complete, ToXContentObject infomationContext) {
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
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        WaitForRolloverReadyStep step = createRandomInstance();

        SetOnce<Exception> exceptionThrown = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean complete, ToXContentObject infomationContext) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionThrown.set(e);
            }
        }, MASTER_TIMEOUT);
        assertThat(exceptionThrown.get().getClass(), equalTo(IllegalArgumentException.class));
        assertThat(exceptionThrown.get().getMessage(), equalTo(String.format(Locale.ROOT,
            "setting [%s] for index [%s] is empty or not defined", RolloverAction.LIFECYCLE_ROLLOVER_ALIAS,
            indexMetadata.getIndex().getName())));
    }

    public void testPerformActionAliasDoesNotPointToIndex() {
        String alias = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        WaitForRolloverReadyStep step = createRandomInstance();

        SetOnce<Exception> exceptionThrown = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean complete, ToXContentObject infomationContext) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionThrown.set(e);
            }
        }, MASTER_TIMEOUT);
        assertThat(exceptionThrown.get().getClass(), equalTo(IllegalArgumentException.class));
        assertThat(exceptionThrown.get().getMessage(), equalTo(String.format(Locale.ROOT,
            "%s [%s] does not point to index [%s]", RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias,
            indexMetadata.getIndex().getName())));
    }
}
