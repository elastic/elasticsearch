/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.tribe;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor.BatchResult;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ClusterStateUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestCustomMetaData;
import org.elasticsearch.tribe.TribeService.TribeNodeClusterStateTaskExecutor;
import org.elasticsearch.tribe.TribeServiceTests.MergableCustomMetaData1;
import org.elasticsearch.tribe.TribeServiceTests.MergableCustomMetaData2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TribeNodeClusterStateTaskExecutorTests extends ESTestCase {

    public void testTribeUpdateNodes() throws Exception {
        String tribeName = "t1";
        TribeNodeClusterStateTaskExecutor executor = new TribeNodeClusterStateTaskExecutor(
                Settings.EMPTY, Collections.emptyList(), tribeName, Collections.emptySet(), logger);
        // add tribe with three nodes
        ClusterState state = ClusterStateUtils.createState(new ClusterName("cluster1"),
                3, false, Collections.emptyList(), random());
        BatchResult<ClusterChangedEvent> batchResult =
                executor.execute(createSimpleClusterState("tribe-node"),
                        Collections.singletonList(new ClusterChangedEvent("test", state, state)));
        assertNotNull(batchResult.resultingState);
        assertThat(batchResult.resultingState.nodes().getSize(), equalTo(state.nodes().getSize()));
        for (DiscoveryNode node : batchResult.resultingState.nodes()) {
            Map<String, String> nodeAttributes = node.getAttributes();
            assertTrue(nodeAttributes.containsKey(TribeService.TRIBE_NAME_SETTING.getKey()));
            assertThat(nodeAttributes.get(TribeService.TRIBE_NAME_SETTING.getKey()), equalTo(tribeName));
        }

        // remove one node
        ClusterState nextState = ClusterStateUtils.nextState(state, true, Collections.emptyList(), Collections.emptyList(), 1);
        batchResult = executor.execute(batchResult.resultingState,
                Collections.singletonList(new ClusterChangedEvent("test", nextState, state)));
        assertNotNull(batchResult.resultingState);
        assertThat(batchResult.resultingState.nodes().getSize(), equalTo(nextState.nodes().getSize()));
        for (DiscoveryNode node : batchResult.resultingState.nodes()) {
            Map<String, String> nodeAttributes = node.getAttributes();
            assertTrue(nodeAttributes.containsKey(TribeService.TRIBE_NAME_SETTING.getKey()));
            assertThat(nodeAttributes.get(TribeService.TRIBE_NAME_SETTING.getKey()), equalTo(tribeName));
        }

        // add one node
        batchResult = executor.execute(createSimpleClusterState("tribe-node"),
                Collections.singletonList(new ClusterChangedEvent("test", state, nextState)));
        assertNotNull(batchResult.resultingState);
        assertThat(batchResult.resultingState.nodes().getSize(), equalTo(state.nodes().getSize()));
        for (DiscoveryNode node : batchResult.resultingState.nodes()) {
            Map<String, String> nodeAttributes = node.getAttributes();
            assertTrue(nodeAttributes.containsKey(TribeService.TRIBE_NAME_SETTING.getKey()));
            assertThat(nodeAttributes.get(TribeService.TRIBE_NAME_SETTING.getKey()), equalTo(tribeName));
        }
    }

    public void testTribeUpdateIndices() throws Exception {
        String tribeName = "t1";
        TribeNodeClusterStateTaskExecutor executor = new TribeNodeClusterStateTaskExecutor(
                Settings.EMPTY, Collections.emptyList(), tribeName, Collections.emptySet(), logger);
        Index initialIndex = new Index("index1", "index1UUID");
        ClusterState state = ClusterStateUtils.createState(new ClusterName("c1"), 3, false,
                Collections.singletonList(initialIndex), random());
        BatchResult<ClusterChangedEvent> batchResult =
                executor.execute(createSimpleClusterState("tribe-node"),
                        Collections.singletonList(new ClusterChangedEvent("test", state, state)));
        assertNotNull(batchResult.resultingState);

        // test index metadata has been added to tribe state
        assertThat(batchResult.resultingState.metaData().getIndices().size(), equalTo(state.metaData().indices().size()));
        assertNotNull(batchResult.resultingState.metaData().index(initialIndex));
        assertNotNull(batchResult.resultingState.routingTable().index(initialIndex));

        ClusterState nextState = ClusterStateUtils.nextState(state, true, Collections.emptyList(),
                Collections.singletonList(initialIndex), 0);

        batchResult = executor.execute(createSimpleClusterState("tribe-node"),
                Collections.singletonList(new ClusterChangedEvent("test", nextState, state)));
        assertNotNull(batchResult.resultingState);

        assertThat(batchResult.resultingState.metaData().getIndices().size(), equalTo(nextState.metaData().indices().size()));
        assertNull(batchResult.resultingState.metaData().index(initialIndex));
        assertNull(batchResult.resultingState.routingTable().index(initialIndex));
    }

    public void testTribeNodeCustomMetaData() throws Exception {
        TribeClusterStateExecutorHolder[] tribeClusterStateExecutorHolders = generateTribeClusterStateExecutors(new String[]{"t1"},
                (tribeName, initialState) ->
                        Collections.singletonList(generateAddingCustomMetaDataEvent(initialState, new MergableCustomMetaData1("data")))
        );

        ClusterState tribeState = createSimpleClusterState("tribe-node");
        BatchResult<ClusterChangedEvent> batchResult = tribeClusterStateExecutorHolders[0].executor.execute(tribeState,
                tribeClusterStateExecutorHolders[0].clusterChangedEvents);
        MetaData.Custom custom = batchResult.resultingState.metaData().custom(MergableCustomMetaData1.TYPE);
        assertNotNull(custom);
        assertThat(((TestCustomMetaData) custom).getData(), equalTo("data"));
    }

    public void testTribeNodeWithSingleClusterCustomMetaDataBatched() throws Exception {
        TribeClusterStateExecutorHolder[] tribeClusterStateExecutorHolders = generateTribeClusterStateExecutors(new String[]{"t1"},
                (tribeName, initialState) -> {
                    List<ClusterChangedEvent> events = new ArrayList<>();
                    List<TestCustomMetaData> changedCustoms = new ArrayList<>();
                    changedCustoms.addAll(generateMergableMD1(randomIntBetween(1, 5)));
                    changedCustoms.addAll(generateMergableMD2(randomIntBetween(1, 5)));
                    Collections.shuffle(changedCustoms, random());
                    ClusterState lastState = initialState;
                    for (TestCustomMetaData changedCustom : changedCustoms) {
                        ClusterChangedEvent event = generateAddingCustomMetaDataEvent(lastState, changedCustom);
                        lastState = event.state();
                        events.add(event);
                    }
                    return events;
                }
        );

        List<ClusterState> latestStates = Stream.of(tribeClusterStateExecutorHolders)
                .map(holder -> holder.clusterChangedEvents)
                .map(clusterChangedEvents -> clusterChangedEvents.get(clusterChangedEvents.size() - 1).state())
                .collect(Collectors.toList());

        ClusterState tribeState = createSimpleClusterState("tribe-node");
        BatchResult<ClusterChangedEvent> batchResult = tribeClusterStateExecutorHolders[0].executor.execute(tribeState,
                tribeClusterStateExecutorHolders[0].clusterChangedEvents);
        ImmutableOpenMap<String, MetaData.Custom> customs = batchResult.resultingState.metaData().customs();
        assertTrue(customs.containsKey(MergableCustomMetaData1.TYPE));
        TestCustomMetaData testCustomMetaData = (TestCustomMetaData) customs.get(MergableCustomMetaData1.TYPE);
        assertThat(testCustomMetaData, equalTo(
                getMergedCustomMetaData(MergableCustomMetaData1.TYPE, latestStates)));
        assertTrue(customs.containsKey(MergableCustomMetaData2.TYPE));
        testCustomMetaData = (TestCustomMetaData) customs.get(MergableCustomMetaData2.TYPE);
        assertThat(testCustomMetaData, equalTo(
                getMergedCustomMetaData(MergableCustomMetaData2.TYPE, latestStates)));
    }

    public void testTribeNodeWithMultipleClusterCustomMetaDataBatched() throws Exception {
        int nClusters = randomIntBetween(2, 5);
        String[] tribeNames = new String[nClusters];
        for (int i = 0; i < nClusters; i++) {
            tribeNames[i] = "t_" + String.valueOf(i);
        }
        TribeClusterStateExecutorHolder[] tribeClusterStateExecutorHolders = generateTribeClusterStateExecutors(tribeNames,
                (tribeName, initialState) -> {
                    List<ClusterChangedEvent> events = new ArrayList<>();
                    List<TestCustomMetaData> changedCustoms = new ArrayList<>();
                    changedCustoms.addAll(generateMergableMD1(randomIntBetween(1, 5)));
                    changedCustoms.addAll(generateMergableMD2(randomIntBetween(1, 5)));
                    Collections.shuffle(changedCustoms, random());
                    ClusterState lastState = initialState;
                    for (TestCustomMetaData changedCustom : changedCustoms) {
                        ClusterChangedEvent event = generateAddingCustomMetaDataEvent(lastState, changedCustom);
                        lastState = event.state();
                        events.add(event);
                    }
                    return events;
                }
        );
        ClusterState tribeState = createSimpleClusterState("tribe-node");
        for (TribeClusterStateExecutorHolder tribeClusterStateExecutorHolder : tribeClusterStateExecutorHolders) {
            BatchResult<ClusterChangedEvent> batchResult = tribeClusterStateExecutorHolder.executor.execute(tribeState,
                    tribeClusterStateExecutorHolder.clusterChangedEvents);
            tribeState = batchResult.resultingState;
        }
        List<ClusterState> latestStates = Stream.of(tribeClusterStateExecutorHolders)
                .map(holder -> holder.clusterChangedEvents)
                .map(clusterChangedEvents -> clusterChangedEvents.get(clusterChangedEvents.size() - 1).state())
                .collect(Collectors.toList());

        ImmutableOpenMap<String, MetaData.Custom> customs = tribeState.metaData().customs();
        assertTrue(customs.containsKey(MergableCustomMetaData1.TYPE));
        TestCustomMetaData testCustomMetaData = (TestCustomMetaData) customs.get(MergableCustomMetaData1.TYPE);
        assertThat(testCustomMetaData, equalTo(
                getMergedCustomMetaData(MergableCustomMetaData1.TYPE, latestStates)));
        assertTrue(customs.containsKey(MergableCustomMetaData2.TYPE));
        testCustomMetaData = (TestCustomMetaData) customs.get(MergableCustomMetaData2.TYPE);
        assertThat(testCustomMetaData, equalTo(
                getMergedCustomMetaData(MergableCustomMetaData2.TYPE, latestStates)));
    }

    public void testTribeNdeWithMultipleClusterIndicesAddAndDelete() throws Exception {
        int nClusters = randomIntBetween(2, 5);
        String[] tribeNames = new String[nClusters];
        for (int i = 0; i < nClusters; i++) {
            tribeNames[i] = "t_" + String.valueOf(i);
        }
        final int nIndex = randomIntBetween(2, 5);
        final int deleteIndex = randomIntBetween(0, nIndex - 1);
        TribeClusterStateExecutorHolder[] tribeClustersAddingIndices = generateTribeClusterStateExecutors(tribeNames,
                (tribeName, initialState) -> {
                    List<ClusterChangedEvent> events = new ArrayList<>();
                    ClusterState lastState = initialState;
                    for (int i = 0; i < nIndex; i++) {
                        String indexName = tribeName + "_index_" + String.valueOf(i);
                        Index index = new Index(indexName, indexName + "UUID");
                        ClusterChangedEvent event = generateAddingIndexEvent(lastState, index);
                        lastState = event.state();
                        events.add(event);
                        if (deleteIndex == i) {
                            // delete a random index after adding
                            ClusterChangedEvent deleteEvent = generateDeletingIndexEvent(lastState, index);
                            lastState = deleteEvent.state();
                            events.add(deleteEvent);
                        }
                    }
                    return events;
                }
        );
        ClusterState tribeState = createSimpleClusterState("tribe-node");
        for (TribeClusterStateExecutorHolder tribeClusterStateExecutorHolder : tribeClustersAddingIndices) {
            BatchResult<ClusterChangedEvent> batchResult = tribeClusterStateExecutorHolder.executor.execute(tribeState,
                    tribeClusterStateExecutorHolder.clusterChangedEvents);
            tribeState = batchResult.resultingState;
        }

        for (String tribeName : tribeNames) {
            for (int i = 0; i < nIndex; i++) {
                String indexName = tribeName + "_index_" + String.valueOf(i);
                Index index = new Index(indexName, indexName + "UUID");
                if (i != deleteIndex) {
                    assertNotNull(tribeState.metaData().index(index));
                    assertNotNull(tribeState.routingTable().index(index));
                } else {
                    assertNull(tribeState.metaData().index(index));
                    assertNull(tribeState.routingTable().index(index));
                }
            }
        }
    }

    private static TribeService.MergableCustomMetaData getMergedCustomMetaData(String type, List<ClusterState> states) {
        TribeService.MergableCustomMetaData mergedCustom = null;
        for (ClusterState state : states) {
            MetaData.Custom custom = state.metaData().custom(type);
            assertNotNull(custom);
            assertThat(custom, instanceOf(TribeService.MergableCustomMetaData.class));
            if (mergedCustom != null) {
                mergedCustom = (TribeService.MergableCustomMetaData) mergedCustom.merge(custom);
            } else {
                mergedCustom = ((TribeService.MergableCustomMetaData) custom);
            }
        }
        return mergedCustom;
    }

    private TribeClusterStateExecutorHolder[] generateTribeClusterStateExecutors(String[] tribeNames,
                                                                                 BiFunction<String, ClusterState,
                                                                                         List<ClusterChangedEvent>>
                                                                                         getClusterChangedEvents) {
        List<Node> nodes = new ArrayList<>();
        TribeNodeClusterStateTaskExecutor[] tribeExecutors = new TribeNodeClusterStateTaskExecutor[tribeNames.length];
        List<ClusterChangedEvent>[] clusterChangedEvents = (List<ClusterChangedEvent>[]) new ArrayList[tribeNames.length];
        for (int i = 0; i < tribeNames.length; i++) {
            String tribeName = tribeNames[i];
            ClusterState initialState = ClusterStateUtils.createState(
                    new ClusterName("cluster-" + tribeName), 3, false, Collections.emptyList(), random());
            List<ClusterChangedEvent> changedEvents = new ArrayList<>(getClusterChangedEvents.apply(tribeName, initialState));
            clusterChangedEvents[i] = changedEvents;
            ClusterState latestState = changedEvents.get(changedEvents.size() - 1).state();
            nodes.add(mockNodeWithState(latestState));
        }
        for (int i = 0; i < tribeNames.length; i++) {
            tribeExecutors[i] = new TribeNodeClusterStateTaskExecutor(
                Settings.EMPTY, nodes, tribeNames[i], Collections.emptySet(), logger);
        }
        TribeClusterStateExecutorHolder[] holders = new TribeClusterStateExecutorHolder[tribeNames.length];
        for (int i = 0; i < tribeNames.length; i++) {
            holders[i] = new TribeClusterStateExecutorHolder(tribeExecutors[i], clusterChangedEvents[i]);
        }
        return holders;
    }

    private static class TribeClusterStateExecutorHolder {
        final TribeNodeClusterStateTaskExecutor executor;
        final List<ClusterChangedEvent> clusterChangedEvents;

        private TribeClusterStateExecutorHolder(TribeNodeClusterStateTaskExecutor executor,
                                                List<ClusterChangedEvent> clusterChangedEvents) {
            this.executor = executor;
            this.clusterChangedEvents = clusterChangedEvents;
        }
    }

    private static List<MergableCustomMetaData1> generateMergableMD1(int size) {
        List<MergableCustomMetaData1> customMetaData = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            customMetaData.add(new MergableCustomMetaData1(randomAsciiOfLength(10)));
        }
        return customMetaData;
    }

    private static List<MergableCustomMetaData2> generateMergableMD2(int size) {
        List<MergableCustomMetaData2> customMetaData = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            customMetaData.add(new MergableCustomMetaData2(randomAsciiOfLength(10)));
        }
        return customMetaData;
    }

    private static ClusterChangedEvent generateAddingCustomMetaDataEvent(ClusterState initialState, TestCustomMetaData customMetaData) {
        ClusterState nextState = ClusterStateUtils.nextState(initialState, Collections.singletonList(customMetaData));
        return new ClusterChangedEvent("adding custom metadata", nextState, initialState);
    }

    private static ClusterChangedEvent generateAddingIndexEvent(ClusterState initialState, Index indexToAdd) {
        List<Index> newIndices = new ArrayList<>();
        for (ObjectCursor<IndexMetaData> existingIndices : initialState.metaData().getIndices().values()) {
            newIndices.add(existingIndices.value.getIndex());
        }
        newIndices.add(indexToAdd);
        ClusterState nextState = ClusterStateUtils.createState(initialState.getClusterName(), 3, false, newIndices, random());
        return new ClusterChangedEvent("adding index", nextState, initialState);
    }

    private static ClusterChangedEvent generateDeletingIndexEvent(ClusterState initialState, Index indexToDelete) {
        List<Index> newIndices = new ArrayList<>();
        for (ObjectCursor<IndexMetaData> existingIndices : initialState.metaData().getIndices().values()) {
            Index index = existingIndices.value.getIndex();
            if (index.equals(indexToDelete) == false) {
                newIndices.add(index);
            }
        }
        ClusterState nextState = ClusterStateUtils.createState(initialState.getClusterName(), 3, false, newIndices, random());
        return new ClusterChangedEvent("delete index", nextState, initialState);
    }

    private static Node mockNodeWithState(ClusterState state) {
        Node mockNode = mock(Node.class);
        Injector mockInjector = mock(Injector.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(state);
        when(mockInjector.getInstance(ClusterService.class)).thenReturn(mockClusterService);
        when(mockNode.injector()).thenReturn(mockInjector);
        return mockNode;
    }

    private static ClusterState createSimpleClusterState(String clusterName) {
        return ClusterState.builder(new ClusterName(clusterName)).build();
    }
}
