/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.RolloverAction;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class RolloverActionTests extends AbstractSerializingTestCase<RolloverAction> {

    @Override
    protected RolloverAction doParseInstance(XContentParser parser) throws IOException {
        return RolloverAction.parse(parser);
    }

    @Override
    protected RolloverAction createTestInstance() {
        String alias = randomAlphaOfLengthBetween(1, 20);
        ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue maxSize = randomBoolean() ? null : new ByteSizeValue(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
        Long maxDocs = randomBoolean() ? null : randomNonNegativeLong();
        TimeValue maxAge = (maxDocs == null && maxSize == null || randomBoolean())
                ? TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test")
                : null;
        return new RolloverAction(alias, maxSize, maxAge, maxDocs);
    }

    @Override
    protected Reader<RolloverAction> instanceReader() {
        return RolloverAction::new;
    }

    @Override
    protected RolloverAction mutateInstance(RolloverAction instance) throws IOException {
        String alias = instance.getAlias();
        ByteSizeValue maxSize = instance.getMaxSize();
        TimeValue maxAge = instance.getMaxAge();
        Long maxDocs = instance.getMaxDocs();
        switch (between(0, 3)) {
        case 0:
            alias = alias + randomAlphaOfLengthBetween(1, 5);
            break;
        case 1:
            maxSize = randomValueOtherThan(maxSize, () -> {
                ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
                return new ByteSizeValue(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
            });
            break;
        case 2:
            maxAge = TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test");
            break;
        case 3:
            maxDocs = randomNonNegativeLong();
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new RolloverAction(alias, maxSize, maxAge, maxDocs);
    }

    public void testNoConditions() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new RolloverAction(randomAlphaOfLengthBetween(1, 20), null, null, null));
        assertEquals("At least one rollover condition must be set.", exception.getMessage());
    }

    public void testNoAlias() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new RolloverAction(null, null, null, 1L));
        assertEquals(RolloverAction.ALIAS_FIELD.getPreferredName() + " must be not be null", exception.getMessage());
    }

//    public void testExecute() throws Exception {
//        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
//
//        RolloverAction action = createTestInstance();
//        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
//                .settings(Settings.builder().put("index.version.created", Version.CURRENT.id))
//                .putAlias(AliasMetaData.builder(action.getAlias()))
//                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
//        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
//                indexMetadata);
//        ClusterState clusterstate = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
//                .build();
//
//        Client client = Mockito.mock(Client.class);
//        AdminClient adminClient = Mockito.mock(AdminClient.class);
//        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
//        ClusterService clusterService = Mockito.mock(ClusterService.class);
//
//        Mockito.when(client.admin()).thenReturn(adminClient);
//        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
//        Mockito.doAnswer(new Answer<Void>() {
//
//            @Override
//            public Void answer(InvocationOnMock invocation) throws Throwable {
//                RolloverRequest request = (RolloverRequest) invocation.getArguments()[0];
//                @SuppressWarnings("unchecked")
//                ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArguments()[1];
//                Set<Condition<?>> expectedConditions = new HashSet<>();
//                if (action.getMaxAge() != null) {
//                    expectedConditions.add(new MaxAgeCondition(action.getMaxAge()));
//                }
//                if (action.getMaxSize() != null) {
//                    expectedConditions.add(new MaxSizeCondition(action.getMaxSize()));
//                }
//                if (action.getMaxDocs() != null) {
//                    expectedConditions.add(new MaxDocsCondition(action.getMaxDocs()));
//                }
//                RolloverIndexTestHelper.assertRolloverIndexRequest(request, action.getAlias(), expectedConditions);
//                listener.onResponse(RolloverIndexTestHelper.createMockResponse(request, true));
//                return null;
//            }
//
//        }).when(indicesClient).rolloverIndex(Mockito.any(), Mockito.any());
//        Mockito.when(clusterService.state()).thenReturn(clusterstate);
//
//        SetOnce<Boolean> actionCompleted = new SetOnce<>();
//        action.execute(index, client, clusterService, new Listener() {
//
//            @Override
//            public void onSuccess(boolean completed) {
//                actionCompleted.set(completed);
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                throw new AssertionError("Unexpected method call", e);
//            }
//        });
//
//        assertEquals(true, actionCompleted.get());
//
//        Mockito.verify(client, Mockito.only()).admin();
//        Mockito.verify(adminClient, Mockito.only()).indices();
//        Mockito.verify(indicesClient, Mockito.only()).rolloverIndex(Mockito.any(), Mockito.any());
//        Mockito.verify(clusterService, Mockito.only()).state();
//    }
//
//    public void testExecuteNotComplete() throws Exception {
//        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
//
//        RolloverAction action = createTestInstance();
//        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
//                .settings(Settings.builder().put("index.version.created", Version.CURRENT.id))
//                .putAlias(AliasMetaData.builder(action.getAlias())).numberOfShards(randomIntBetween(1, 5))
//                .numberOfReplicas(randomIntBetween(0, 5)).build();
//        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
//                indexMetadata);
//        ClusterState clusterstate = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
//                .build();
//
//        Client client = Mockito.mock(Client.class);
//        AdminClient adminClient = Mockito.mock(AdminClient.class);
//        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
//        ClusterService clusterService = Mockito.mock(ClusterService.class);
//
//        Mockito.when(client.admin()).thenReturn(adminClient);
//        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
//        Mockito.doAnswer(new Answer<Void>() {
//
//            @Override
//            public Void answer(InvocationOnMock invocation) throws Throwable {
//                RolloverRequest request = (RolloverRequest) invocation.getArguments()[0];
//                @SuppressWarnings("unchecked")
//                ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArguments()[1];
//                Set<Condition<?>> expectedConditions = new HashSet<>();
//                if (action.getMaxAge() != null) {
//                    expectedConditions.add(new MaxAgeCondition(action.getMaxAge()));
//                }
//                if (action.getMaxSize() != null) {
//                    expectedConditions.add(new MaxSizeCondition(action.getMaxSize()));
//                }
//                if (action.getMaxDocs() != null) {
//                    expectedConditions.add(new MaxDocsCondition(action.getMaxDocs()));
//                }
//                RolloverIndexTestHelper.assertRolloverIndexRequest(request, action.getAlias(), expectedConditions);
//                listener.onResponse(RolloverIndexTestHelper.createMockResponse(request, false));
//                return null;
//            }
//
//        }).when(indicesClient).rolloverIndex(Mockito.any(), Mockito.any());
//        Mockito.when(clusterService.state()).thenReturn(clusterstate);
//
//        SetOnce<Boolean> actionCompleted = new SetOnce<>();
//        action.execute(index, client, clusterService, new Listener() {
//
//            @Override
//            public void onSuccess(boolean completed) {
//                actionCompleted.set(completed);
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                throw new AssertionError("Unexpected method call", e);
//            }
//        });
//
//        assertEquals(false, actionCompleted.get());
//
//        Mockito.verify(client, Mockito.only()).admin();
//        Mockito.verify(adminClient, Mockito.only()).indices();
//        Mockito.verify(indicesClient, Mockito.only()).rolloverIndex(Mockito.any(), Mockito.any());
//        Mockito.verify(clusterService, Mockito.only()).state();
//    }
//
//    public void testExecuteAlreadyCompleted() throws Exception {
//        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
//
//        RolloverAction action = createTestInstance();
//
//        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
//                .settings(Settings.builder().put("index.version.created", Version.CURRENT.id)).numberOfShards(randomIntBetween(1, 5))
//                .numberOfReplicas(randomIntBetween(0, 5)).build();
//        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
//                indexMetadata);
//        ClusterState clusterstate = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
//                .build();
//
//        Client client = Mockito.mock(Client.class);
//        AdminClient adminClient = Mockito.mock(AdminClient.class);
//        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
//        ClusterService clusterService = Mockito.mock(ClusterService.class);
//
//        Mockito.when(clusterService.state()).thenReturn(clusterstate);
//
//        SetOnce<Boolean> actionCompleted = new SetOnce<>();
//        action.execute(index, client, clusterService, new Listener() {
//
//            @Override
//            public void onSuccess(boolean completed) {
//                actionCompleted.set(completed);
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                throw new AssertionError("Unexpected method call", e);
//            }
//        });
//
//        assertEquals(true, actionCompleted.get());
//
//        Mockito.verify(clusterService, Mockito.only()).state();
//        Mockito.verifyZeroInteractions(client, adminClient, indicesClient);
//    }
//
//    public void testExecuteFailure() throws Exception {
//        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
//        Exception exception = new RuntimeException();
//
//        RolloverAction action = createTestInstance();
//
//        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
//                .settings(Settings.builder().put("index.version.created", Version.CURRENT.id))
//                .putAlias(AliasMetaData.builder(action.getAlias())).numberOfShards(randomIntBetween(1, 5))
//                .numberOfReplicas(randomIntBetween(0, 5)).build();
//        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
//                indexMetadata);
//        ClusterState clusterstate = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
//                .build();
//
//        Client client = Mockito.mock(Client.class);
//        AdminClient adminClient = Mockito.mock(AdminClient.class);
//        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
//        ClusterService clusterService = Mockito.mock(ClusterService.class);
//
//        Mockito.when(client.admin()).thenReturn(adminClient);
//        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
//        Mockito.doAnswer(new Answer<Void>() {
//
//            @Override
//            public Void answer(InvocationOnMock invocation) throws Throwable {
//                RolloverRequest request = (RolloverRequest) invocation.getArguments()[0];
//                @SuppressWarnings("unchecked")
//                ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArguments()[1];
//                Set<Condition<?>> expectedConditions = new HashSet<>();
//                if (action.getMaxAge() != null) {
//                    expectedConditions.add(new MaxAgeCondition(action.getMaxAge()));
//                }
//                if (action.getMaxSize() != null) {
//                    expectedConditions.add(new MaxSizeCondition(action.getMaxSize()));
//                }
//                if (action.getMaxDocs() != null) {
//                    expectedConditions.add(new MaxDocsCondition(action.getMaxDocs()));
//                }
//                RolloverIndexTestHelper.assertRolloverIndexRequest(request, action.getAlias(), expectedConditions);
//                listener.onFailure(exception);
//                return null;
//            }
//
//        }).when(indicesClient).rolloverIndex(Mockito.any(), Mockito.any());
//        Mockito.when(clusterService.state()).thenReturn(clusterstate);
//
//        SetOnce<Boolean> exceptionThrown = new SetOnce<>();
//        action.execute(index, client, clusterService, new Listener() {
//
//            @Override
//            public void onSuccess(boolean completed) {
//                throw new AssertionError("Unexpected method call");
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                assertEquals(exception, e);
//                exceptionThrown.set(true);
//            }
//        });
//
//        assertEquals(true, exceptionThrown.get());
//
//        Mockito.verify(client, Mockito.only()).admin();
//        Mockito.verify(adminClient, Mockito.only()).indices();
//        Mockito.verify(indicesClient, Mockito.only()).rolloverIndex(Mockito.any(), Mockito.any());
//    }

}
