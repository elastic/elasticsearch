/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;


import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.protocol.xpack.frozen.FreezeResponse;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.equalTo;

public class FreezeStepTests extends AbstractStepMasterTimeoutTestCase<FreezeStep> {

    @Override
    public FreezeStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();

        return new FreezeStep(stepKey, nextStepKey, client);
    }

    @Override
    public FreezeStep mutateInstance(FreezeStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
        case 0:
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 1:
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new FreezeStep(key, nextKey, instance.getClient());
    }

    @Override
    public FreezeStep copyInstance(FreezeStep instance) {
        return new FreezeStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    @Override
    protected IndexMetadata getIndexMetadata() {
        return IndexMetadata.builder(randomAlphaOfLength(10)).settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
    }

    public void testIndexSurvives() {
        assertTrue(createRandomInstance().indexSurvives());
    }

    public void testFreeze() {
        IndexMetadata indexMetadata = getIndexMetadata();

        Mockito.doAnswer(invocation -> {
            assertSame(invocation.getArguments()[0], FreezeIndexAction.INSTANCE);
            FreezeRequest request = (FreezeRequest) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            assertNotNull(request);
            assertEquals(1, request.indices().length);
            assertEquals(indexMetadata.getIndex().getName(), request.indices()[0]);
            listener.onResponse(new FreezeResponse(true, true));
            return null;
        }).when(indicesClient).execute(Mockito.any(), Mockito.any(), Mockito.any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();

        FreezeStep step = createRandomInstance();
        step.performAction(indexMetadata, emptyClusterState(), null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                actionCompleted.set(complete);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });

        assertThat(actionCompleted.get(), equalTo(true));

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).execute(Mockito.any(), Mockito.any(), Mockito.any());
    }

    public void testExceptionThrown() {
        IndexMetadata indexMetadata = getIndexMetadata();
        Exception exception = new RuntimeException();

        Mockito.doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).execute(Mockito.any(), Mockito.any(), Mockito.any());

        SetOnce<Boolean> exceptionThrown = new SetOnce<>();
        FreezeStep step = createRandomInstance();
        step.performAction(indexMetadata, emptyClusterState(), null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(exception, e);
                exceptionThrown.set(true);
            }
        });

        assertThat(exceptionThrown.get(), equalTo(true));
    }

    public void testNotAcknowledged() {
        IndexMetadata indexMetadata = getIndexMetadata();

        Mockito.doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            listener.onResponse(new FreezeResponse(false, false));
            return null;
        }).when(indicesClient).execute(Mockito.any(), Mockito.any(), Mockito.any());

        SetOnce<Boolean> exceptionThrown = new SetOnce<>();
        FreezeStep step = createRandomInstance();
        step.performAction(indexMetadata, emptyClusterState(), null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals("freeze index request failed to be acknowledged", e.getMessage());
                exceptionThrown.set(true);
            }
        });

        assertThat(exceptionThrown.get(), equalTo(true));
    }
}
