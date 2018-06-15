/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;


import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsTestHelper;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.indexlifecycle.AsyncActionStep.Listener;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class UpdateRolloverLifecycleDateStepTests extends AbstractStepTestCase<UpdateRolloverLifecycleDateStep> {

    private Client client;

    @Before
    public void setup() {
        client = Mockito.mock(Client.class);
    }

    @Override
    public UpdateRolloverLifecycleDateStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new UpdateRolloverLifecycleDateStep(stepKey, nextStepKey, client);
    }

    @Override
    public UpdateRolloverLifecycleDateStep mutateInstance(UpdateRolloverLifecycleDateStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        }

        return new UpdateRolloverLifecycleDateStep(key, nextKey, client);
    }

    @Override
    public UpdateRolloverLifecycleDateStep copyInstance(UpdateRolloverLifecycleDateStep instance) {
        return new UpdateRolloverLifecycleDateStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    @SuppressWarnings("unchecked")
    public void testPerformAction() {
        String alias = randomAlphaOfLength(3);
        long creationDate = randomLongBetween(0, 1000000);
        long rolloverTime = randomValueOtherThan(creationDate, () -> randomNonNegativeLong());
        IndexMetaData newIndexMetaData = IndexMetaData.builder(randomAlphaOfLength(11))
            .settings(settings(Version.CURRENT)).creationDate(creationDate)
            .putAlias(AliasMetaData.builder(alias)).numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5)).build();
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10))
            .putRolloverInfo(new RolloverInfo(alias, Collections.emptyList(), rolloverTime))
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder()
                .put(indexMetaData, false)
                .put(newIndexMetaData, false)).build();

        UpdateRolloverLifecycleDateStep step = createRandomInstance();
        Settings expectedSettings = Settings.builder().put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, rolloverTime).build();

        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.doAnswer((invocation) -> {
            UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
            ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocation.getArguments()[1];
            UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexMetaData.getIndex().getName());
            listener.onResponse(UpdateSettingsTestHelper.createMockResponse(true));
            return null;
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();

        step.performAction(indexMetaData, clusterState, new Listener() {

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
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testPerformActionBeforeRolloverHappened() {
        String alias = randomAlphaOfLength(3);
        long creationDate = randomLongBetween(0, 1000000);
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(11))
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .creationDate(creationDate).putAlias(AliasMetaData.builder(alias)).numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder().put(indexMetaData, false)).build();
        UpdateRolloverLifecycleDateStep step = createRandomInstance();

        SetOnce<Exception> exceptionThrown = new SetOnce<>();
        step.performAction(indexMetaData, clusterState, new Listener() {

            @Override
            public void onResponse(boolean complete) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionThrown.set(e);
            }
        });

        assertThat(exceptionThrown.get().getClass(), equalTo(IllegalStateException.class));
        assertThat(exceptionThrown.get().getMessage(),
            equalTo("index [" + indexMetaData.getIndex().getName() + "] has not rolled over yet"));
    }

    public void testPerformActionWithNoRolloverAliasSetting() {
        long creationDate = randomLongBetween(0, 1000000);
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(11))
            .settings(settings(Version.CURRENT)).creationDate(creationDate).numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder().put(indexMetaData, false)).build();
        UpdateRolloverLifecycleDateStep step = createRandomInstance();

        SetOnce<Exception> exceptionThrown = new SetOnce<>();
        step.performAction(indexMetaData, clusterState, new Listener() {

            @Override
            public void onResponse(boolean complete) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionThrown.set(e);
            }
        });

        assertThat(exceptionThrown.get().getClass(), equalTo(IllegalStateException.class));
        assertThat(exceptionThrown.get().getMessage(),
            equalTo("setting [index.lifecycle.rollover_alias] is not set on index [" + indexMetaData.getIndex().getName() +"]"));
    }

    @SuppressWarnings("unchecked")
    public void testPerformActionUpdateFailure() {
        Exception exception = new RuntimeException();
        String alias = randomAlphaOfLength(3);
        long creationDate = randomLongBetween(0, 1000000);
        long rolloverTime = randomValueOtherThan(creationDate, () -> randomNonNegativeLong());
        IndexMetaData newIndexMetaData = IndexMetaData.builder(randomAlphaOfLength(11))
            .settings(settings(Version.CURRENT)).creationDate(creationDate)
            .putAlias(AliasMetaData.builder(alias)) .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5)).build();
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10))
            .putRolloverInfo(new RolloverInfo(alias, Collections.emptyList(), rolloverTime))
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder()
                .put(indexMetaData, false)
                .put(newIndexMetaData, false)).build();

        UpdateRolloverLifecycleDateStep step = createRandomInstance();
        Settings expectedSettings = Settings.builder().put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, rolloverTime).build();

        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
        Mockito.doAnswer((invocation) -> {
            UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
            ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocation.getArguments()[1];
            UpdateSettingsTestHelper.assertSettingsRequest(request, expectedSettings, indexMetaData.getIndex().getName());
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        SetOnce<Boolean> exceptionThrown = new SetOnce<>();
        step.performAction(indexMetaData, clusterState, new Listener() {

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
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }
}
