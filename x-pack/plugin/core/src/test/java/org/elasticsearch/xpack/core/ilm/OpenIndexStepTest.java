/*
 * Copyright (c) 2019. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.hamcrest.Matchers.equalTo;

/**
 * Created by sivagurunathanvelayutham on Dec, 2019
 */
public class OpenIndexStepTest extends AbstractStepTestCase<OpenIndexStep> {

    private Client client;

    @Before
    public void setup() {
        client = Mockito.mock(Client.class);
    }

    @Override
    protected OpenIndexStep createRandomInstance() {
        return new OpenIndexStep(randomStepKey(), randomStepKey(), client);
    }

    @Override
    protected OpenIndexStep mutateInstance(OpenIndexStep instance) {
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

        return new OpenIndexStep(key, nextKey, client);
    }

    @Override
    protected OpenIndexStep copyInstance(OpenIndexStep instance) {
        return new OpenIndexStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    public void testPerformAction() {
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10)).settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .state(IndexMetaData.State.CLOSE)
            .build();

        OpenIndexStep step = createRandomInstance();

        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Mockito.doAnswer((Answer<Void>) invocation -> {
            OpenIndexRequest request = (OpenIndexRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<OpenIndexResponse> listener = (ActionListener<OpenIndexResponse>) invocation.getArguments()[1];
            assertThat(request.indices(), equalTo(new String[]{indexMetaData.getIndex().getName()}));
            listener.onResponse(new OpenIndexResponse(true, true));
            return null;
        }).when(indicesClient).open(Mockito.any(), Mockito.any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();

        step.performAction(indexMetaData, null, null, new AsyncActionStep.Listener() {

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
        Mockito.verify(indicesClient, Mockito.only()).open(Mockito.any(), Mockito.any());
    }


    public void testPerformActionFailure() {
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10)).settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .state(IndexMetaData.State.CLOSE)
            .build();

        OpenIndexStep step = createRandomInstance();
        Exception exception = new RuntimeException();
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Mockito.doAnswer((Answer<Void>) invocation -> {
            OpenIndexRequest request = (OpenIndexRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<OpenIndexResponse> listener = (ActionListener<OpenIndexResponse>) invocation.getArguments()[1];
            assertThat(request.indices(), equalTo(new String[]{indexMetaData.getIndex().getName()}));
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).open(Mockito.any(), Mockito.any());

        SetOnce<Boolean> exceptionThrown = new SetOnce<>();

        step.performAction(indexMetaData, null, null, new AsyncActionStep.Listener() {

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
        Mockito.verify(indicesClient, Mockito.only()).open(Mockito.any(), Mockito.any());
    }
}
