/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverIndexTestHelper;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.indexlifecycle.AsyncActionStep.Listener;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashSet;
import java.util.Set;

public class RolloverStepTests extends ESTestCase {

    private Client client;

    @Before
    public void setup() {
        client = Mockito.mock(Client.class);
    }

    public RolloverStep createRandomInstance() {
        StepKey stepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
        StepKey nextStepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
        String alias = randomAlphaOfLengthBetween(1, 20);
        ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue maxSize = randomBoolean() ? null : new ByteSizeValue(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
        Long maxDocs = randomBoolean() ? null : randomNonNegativeLong();
        TimeValue maxAge = (maxDocs == null && maxSize == null || randomBoolean())
                ? TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test")
                : null;
        return new RolloverStep(stepKey, nextStepKey, client, alias, maxSize, maxAge, maxDocs);
    }

    public RolloverStep mutateInstance(RolloverStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        String alias = instance.getAlias();
        ByteSizeValue maxSize = instance.getMaxSize();
        TimeValue maxAge = instance.getMaxAge();
        Long maxDocs = instance.getMaxDocs();

        switch (between(0, 5)) {
        case 0:
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 1:
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 2:
            alias = alias + randomAlphaOfLengthBetween(1, 5);
            break;
        case 3:
            maxSize = randomValueOtherThan(maxSize, () -> {
                ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
                return new ByteSizeValue(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
            });
            break;
        case 4:
            maxAge = TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test");
            break;
        case 5:
            maxDocs = randomNonNegativeLong();
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new RolloverStep(key, nextKey, instance.getClient(), alias, maxSize, maxAge, maxDocs);
    }

    public void testHashcodeAndEquals() {
        EqualsHashCodeTestUtils
                .checkEqualsAndHashCode(createRandomInstance(),
                        instance -> new RolloverStep(instance.getKey(), instance.getNextStepKey(), instance.getClient(),
                                instance.getAlias(), instance.getMaxSize(), instance.getMaxAge(), instance.getMaxDocs()),
                        this::mutateInstance);
    }

    public void testPerformAction() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));

        RolloverStep step = createRandomInstance();

        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
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
                RolloverIndexTestHelper.assertRolloverIndexRequest(request, step.getAlias(), expectedConditions);
                listener.onResponse(RolloverIndexTestHelper.createMockResponse(request, true));
                return null;
            }

        }).when(indicesClient).rolloverIndex(Mockito.any(), Mockito.any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        step.performAction(index, new Listener() {

            @Override
            public void onResponse(boolean complete) {
                actionCompleted.set(complete);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertEquals(true, actionCompleted.get());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).rolloverIndex(Mockito.any(), Mockito.any());
    }

    public void testPerformActionNotComplete() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));

        RolloverStep step = createRandomInstance();

        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
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
                RolloverIndexTestHelper.assertRolloverIndexRequest(request, step.getAlias(), expectedConditions);
                listener.onResponse(RolloverIndexTestHelper.createMockResponse(request, false));
                return null;
            }

        }).when(indicesClient).rolloverIndex(Mockito.any(), Mockito.any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        step.performAction(index, new Listener() {

            @Override
            public void onResponse(boolean complete) {
                actionCompleted.set(complete);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertEquals(false, actionCompleted.get());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).rolloverIndex(Mockito.any(), Mockito.any());
    }

    public void testPerformActionFailure() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Exception exception = new RuntimeException();
        RolloverStep step = createRandomInstance();

        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
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
                RolloverIndexTestHelper.assertRolloverIndexRequest(request, step.getAlias(), expectedConditions);
                listener.onFailure(exception);
                return null;
            }

        }).when(indicesClient).rolloverIndex(Mockito.any(), Mockito.any());

        SetOnce<Boolean> exceptionThrown = new SetOnce<>();
        step.performAction(index, new Listener() {

            @Override
            public void onResponse(boolean complete) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                assertSame(exception, e);
                exceptionThrown.set(true);
            }
        });

        assertEquals(true, exceptionThrown.get());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).rolloverIndex(Mockito.any(), Mockito.any());
    }

}
