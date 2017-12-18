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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TemplateUpgradeServiceTests extends ESTestCase {

    private final ClusterService clusterService = new ClusterService(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
        ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null, Collections.emptyMap());

    public void testCalculateChangesAddChangeAndDelete() {

        boolean shouldAdd = randomBoolean();
        boolean shouldRemove = randomBoolean();
        boolean shouldChange = randomBoolean();

        MetaData metaData = randomMetaData(
            IndexTemplateMetaData.builder("user_template").patterns(randomIndexPatterns()).build(),
            IndexTemplateMetaData.builder("removed_test_template").patterns(randomIndexPatterns()).build(),
            IndexTemplateMetaData.builder("changed_test_template").patterns(randomIndexPatterns()).build()
        );

        TemplateUpgradeService service = new TemplateUpgradeService(Settings.EMPTY, null, clusterService, null,
            Arrays.asList(
                templates -> {
                    if (shouldAdd) {
                        assertNull(templates.put("added_test_template",
                            IndexTemplateMetaData.builder("added_test_template").patterns(randomIndexPatterns()).build()));
                    }
                    return templates;
                },
                templates -> {
                    if (shouldRemove) {
                        assertNotNull(templates.remove("removed_test_template"));
                    }
                    return templates;
                },
                templates -> {
                    if (shouldChange) {
                        assertNotNull(templates.put("changed_test_template",
                            IndexTemplateMetaData.builder("changed_test_template").patterns(randomIndexPatterns()).order(10).build()));
                    }
                    return templates;
                }
            ));

        Optional<Tuple<Map<String, BytesReference>, Set<String>>> optChanges =
            service.calculateTemplateChanges(metaData.templates());

        if (shouldAdd || shouldRemove || shouldChange) {
            Tuple<Map<String, BytesReference>, Set<String>> changes = optChanges.orElseThrow(() ->
                new AssertionError("Should have non empty changes"));
            if (shouldAdd) {
                assertThat(changes.v1().get("added_test_template"), notNullValue());
                if (shouldChange) {
                    assertThat(changes.v1().keySet(), hasSize(2));
                    assertThat(changes.v1().get("changed_test_template"), notNullValue());
                } else {
                    assertThat(changes.v1().keySet(), hasSize(1));
                }
            } else {
                if (shouldChange) {
                    assertThat(changes.v1().get("changed_test_template"), notNullValue());
                    assertThat(changes.v1().keySet(), hasSize(1));
                } else {
                    assertThat(changes.v1().keySet(), empty());
                }
            }

            if (shouldRemove) {
                assertThat(changes.v2(), hasSize(1));
                assertThat(changes.v2().contains("removed_test_template"), equalTo(true));
            } else {
                assertThat(changes.v2(), empty());
            }
        } else {
            assertThat(optChanges.isPresent(), equalTo(false));
        }
    }


    @SuppressWarnings("unchecked")
    public void testUpdateTemplates() {
        int additionsCount = randomIntBetween(0, 5);
        int deletionsCount = randomIntBetween(0, 3);

        List<ActionListener<PutIndexTemplateResponse>> putTemplateListeners = new ArrayList<>();
        List<ActionListener<DeleteIndexTemplateResponse>> deleteTemplateListeners = new ArrayList<>();

        Client mockClient = mock(Client.class);
        AdminClient mockAdminClient = mock(AdminClient.class);
        IndicesAdminClient mockIndicesAdminClient = mock(IndicesAdminClient.class);
        when(mockClient.admin()).thenReturn(mockAdminClient);
        when(mockAdminClient.indices()).thenReturn(mockIndicesAdminClient);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            PutIndexTemplateRequest request = (PutIndexTemplateRequest) args[0];
            assertThat(request.name(), equalTo("add_template_" + request.order()));
            putTemplateListeners.add((ActionListener) args[1]);
            return null;
        }).when(mockIndicesAdminClient).putTemplate(any(PutIndexTemplateRequest.class), any(ActionListener.class));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            DeleteIndexTemplateRequest request = (DeleteIndexTemplateRequest) args[0];
            assertThat(request.name(), startsWith("remove_template_"));
            deleteTemplateListeners.add((ActionListener) args[1]);
            return null;
        }).when(mockIndicesAdminClient).deleteTemplate(any(DeleteIndexTemplateRequest.class), any(ActionListener.class));

        Set<String> deletions = new HashSet<>(deletionsCount);
        for (int i = 0; i < deletionsCount; i++) {
            deletions.add("remove_template_" + i);
        }
        Map<String, BytesReference> additions = new HashMap<>(additionsCount);
        for (int i = 0; i < additionsCount; i++) {
            additions.put("add_template_" + i, new BytesArray("{\"index_patterns\" : \"*\", \"order\" : " + i + "}"));
        }

        TemplateUpgradeService service = new TemplateUpgradeService(Settings.EMPTY, mockClient, clusterService, null,
            Collections.emptyList());

        service.updateTemplates(additions, deletions);
        final int updatesInProgress = service.getUpdatesInProgress();
        assertEquals(0, updatesInProgress);

        assertThat(putTemplateListeners, hasSize(additionsCount));
        assertThat(deleteTemplateListeners, hasSize(deletionsCount));

        for (int i = 0; i < additionsCount; i++) {
            if (randomBoolean()) {
                putTemplateListeners.get(i).onFailure(new RuntimeException("test - ignore"));
            } else {
                putTemplateListeners.get(i).onResponse(new PutIndexTemplateResponse(randomBoolean()) {

                });
            }
        }

        for (int i = 0; i < deletionsCount; i++) {
            if (randomBoolean()) {
                int prevUpdatesInProgress = service.getUpdatesInProgress();
                deleteTemplateListeners.get(i).onFailure(new RuntimeException("test - ignore"));
                assertThat(prevUpdatesInProgress - service.getUpdatesInProgress(), equalTo(1));
            } else {
                int prevUpdatesInProgress = service.getUpdatesInProgress();
                deleteTemplateListeners.get(i).onResponse(new DeleteIndexTemplateResponse(randomBoolean()) {

                });
                assertThat(prevUpdatesInProgress - service.getUpdatesInProgress(), equalTo(1));
            }
        }
        assertThat(updatesInProgress - service.getUpdatesInProgress(), equalTo(additionsCount + deletionsCount));
    }

    public void testUpdateChecksForMoreUpdatesAfterCompletion() {
        final int additionsCount = randomIntBetween(2, 5);

        List<ActionListener<PutIndexTemplateResponse>> putTemplateListeners = new ArrayList<>();

        Client mockClient = mock(Client.class);
        AdminClient mockAdminClient = mock(AdminClient.class);
        IndicesAdminClient mockIndicesAdminClient = mock(IndicesAdminClient.class);
        when(mockClient.admin()).thenReturn(mockAdminClient);
        when(mockAdminClient.indices()).thenReturn(mockIndicesAdminClient);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            PutIndexTemplateRequest request = (PutIndexTemplateRequest) args[0];
            assertThat(request.name(), equalTo("add_template_" + request.order()));
            putTemplateListeners.add((ActionListener) args[1]);
            return null;
        }).when(mockIndicesAdminClient).putTemplate(any(PutIndexTemplateRequest.class), any(ActionListener.class));

        final Map<String, IndexTemplateMetaData> additions = new HashMap<>(additionsCount);
        for (int i = 0; i < additionsCount; i++) {
            String name = "add_template_" + i;
            additions.put(name, IndexTemplateMetaData.builder(name)
                .patterns(Collections.singletonList("*"))
                .order(i)
                .build());
        }

        final List<Tuple<Map<String, BytesReference>, Set<String>>> executeParams = new ArrayList<>();
        final ClusterService mockClusterService = mock(ClusterService.class);
        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch"))
            .nodes(DiscoveryNodes.builder()
                .add(DiscoveryNode.createLocal(Settings.EMPTY, new TransportAddress(TransportAddress.META_ADDRESS, 0), "id"))
                .masterNodeId("id")
                .localNodeId("id")
                .build())
            .build();
        when(mockClusterService.state()).thenReturn(state);
        TemplateUpgradeService service = new TemplateUpgradeService(Settings.EMPTY, mockClient, mockClusterService, null,
            Collections.singletonList(templates -> {
                templates.putAll(additions);
                return templates;
            })) {

            @Override
            void execute(Map<String, BytesReference> changes, Set<String> deletions) {
                executeParams.add(new Tuple<>(changes, deletions));
                updateTemplates(changes, deletions); // the real code forks a thread to do this but to simplify testing, just call here
            }
        };

        service.clusterChanged(new ClusterChangedEvent("template upgrade test", state, ClusterState.EMPTY_STATE));
        final int updatesInProgress = service.getUpdatesInProgress();
        assertEquals(additionsCount, updatesInProgress);
        assertThat(putTemplateListeners, hasSize(additionsCount));
        assertEquals(1, executeParams.size());
        Tuple<Map<String, BytesReference>, Set<String>> initParams = executeParams.get(0);
        assertEquals(additionsCount, initParams.v1().size());
        assertEquals(0, initParams.v2().size());
        executeParams.remove(initParams);
        assertEquals(0, executeParams.size());

        final int numMissing = randomIntBetween(1, additionsCount - 1);
        Map<String, IndexTemplateMetaData> mapWithMissing = new HashMap<>(additions);
        for (int i = 0; i < numMissing; i++) {
            mapWithMissing.remove("add_template_" + i);
        }
        ImmutableOpenMap<String, IndexTemplateMetaData> templatesMetadata = ImmutableOpenMap.<String, IndexTemplateMetaData>builder()
            .putAll(mapWithMissing)
            .build();
        state = ClusterState.builder(state)
            .metaData(MetaData.builder().templates(templatesMetadata))
            .build();
        when(mockClusterService.state()).thenReturn(state);

        for (int i = 0; i < additionsCount; i++) {
            assertEquals(0, executeParams.size()); // verify execute is not called before all listeners are complete
            if (randomBoolean()) {
                putTemplateListeners.get(i).onFailure(new RuntimeException("test - ignore"));
            } else {
                putTemplateListeners.get(i).onResponse(new PutIndexTemplateResponse(randomBoolean()) {

                });
            }
        }

        assertEquals(1, executeParams.size());
        Tuple<Map<String, BytesReference>, Set<String>> recheckParams = executeParams.get(0);
        assertEquals(numMissing, recheckParams.v1().size());
        assertEquals(0, recheckParams.v2().size());
    }

    private static final Set<DiscoveryNode.Role> MASTER_DATA_ROLES =
        Collections.unmodifiableSet(EnumSet.of(DiscoveryNode.Role.MASTER, DiscoveryNode.Role.DATA));

    @SuppressWarnings("unchecked")
    public void testClusterStateUpdate() {

        AtomicReference<ActionListener<PutIndexTemplateResponse>> addedListener = new AtomicReference<>();
        AtomicReference<ActionListener<PutIndexTemplateResponse>> changedListener = new AtomicReference<>();
        AtomicReference<ActionListener<DeleteIndexTemplateResponse>> removedListener = new AtomicReference<>();
        AtomicInteger updateInvocation = new AtomicInteger();

        MetaData metaData = randomMetaData(
            IndexTemplateMetaData.builder("user_template").patterns(randomIndexPatterns()).build(),
            IndexTemplateMetaData.builder("removed_test_template").patterns(randomIndexPatterns()).build(),
            IndexTemplateMetaData.builder("changed_test_template").patterns(randomIndexPatterns()).build()
        );

        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        when(threadPool.generic()).thenReturn(executorService);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 1;
            Runnable runnable = (Runnable) args[0];
            runnable.run();
            updateInvocation.incrementAndGet();
            return null;
        }).when(executorService).execute(any(Runnable.class));

        Client mockClient = mock(Client.class);
        AdminClient mockAdminClient = mock(AdminClient.class);
        IndicesAdminClient mockIndicesAdminClient = mock(IndicesAdminClient.class);
        when(mockClient.admin()).thenReturn(mockAdminClient);
        when(mockAdminClient.indices()).thenReturn(mockIndicesAdminClient);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            PutIndexTemplateRequest request = (PutIndexTemplateRequest) args[0];
            if (request.name().equals("added_test_template")) {
                assertThat(addedListener.getAndSet((ActionListener) args[1]), nullValue());
            } else if (request.name().equals("changed_test_template")) {
                assertThat(changedListener.getAndSet((ActionListener) args[1]), nullValue());
            } else {
                fail("unexpected put template call for " + request.name());
            }
            return null;
        }).when(mockIndicesAdminClient).putTemplate(any(PutIndexTemplateRequest.class), any(ActionListener.class));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            DeleteIndexTemplateRequest request = (DeleteIndexTemplateRequest) args[0];
            assertThat(request.name(), startsWith("removed_test_template"));
            assertThat(removedListener.getAndSet((ActionListener) args[1]), nullValue());
            return null;
        }).when(mockIndicesAdminClient).deleteTemplate(any(DeleteIndexTemplateRequest.class), any(ActionListener.class));

        TemplateUpgradeService service = new TemplateUpgradeService(Settings.EMPTY, mockClient, clusterService, threadPool,
            Arrays.asList(
                templates -> {
                    assertNull(templates.put("added_test_template", IndexTemplateMetaData.builder("added_test_template")
                        .patterns(Collections.singletonList("*")).build()));
                    return templates;
                },
                templates -> {
                    assertNotNull(templates.remove("removed_test_template"));
                    return templates;
                },
                templates -> {
                    assertNotNull(templates.put("changed_test_template", IndexTemplateMetaData.builder("changed_test_template")
                        .patterns(Collections.singletonList("*")).order(10).build()));
                    return templates;
                }
            ));

        ClusterState prevState = ClusterState.EMPTY_STATE;
        ClusterState state = ClusterState.builder(prevState).nodes(DiscoveryNodes.builder()
            .add(new DiscoveryNode("node1", "node1", buildNewFakeTransportAddress(), emptyMap(), MASTER_DATA_ROLES, Version.CURRENT)
            ).localNodeId("node1").masterNodeId("node1").build()
        ).metaData(metaData).build();
        service.clusterChanged(new ClusterChangedEvent("test", state, prevState));

        assertThat(updateInvocation.get(), equalTo(1));
        assertThat(addedListener.get(), notNullValue());
        assertThat(changedListener.get(), notNullValue());
        assertThat(removedListener.get(), notNullValue());

        prevState = state;
        state = ClusterState.builder(prevState).metaData(MetaData.builder(state.metaData()).removeTemplate("user_template")).build();
        service.clusterChanged(new ClusterChangedEvent("test 2", state, prevState));

        // Make sure that update wasn't invoked since we are still running
        assertThat(updateInvocation.get(), equalTo(1));

        addedListener.getAndSet(null).onResponse(new PutIndexTemplateResponse(true) {
        });
        changedListener.getAndSet(null).onResponse(new PutIndexTemplateResponse(true) {
        });
        removedListener.getAndSet(null).onResponse(new DeleteIndexTemplateResponse(true) {
        });

        service.clusterChanged(new ClusterChangedEvent("test 3", state, prevState));

        // Make sure that update was called this time since we are no longer running
        assertThat(updateInvocation.get(), equalTo(2));

        addedListener.getAndSet(null).onFailure(new RuntimeException("test - ignore"));
        changedListener.getAndSet(null).onFailure(new RuntimeException("test - ignore"));
        removedListener.getAndSet(null).onFailure(new RuntimeException("test - ignore"));

        service.clusterChanged(new ClusterChangedEvent("test 3", state, prevState));

        // Make sure that update wasn't called this time since the index template metadata didn't change
        assertThat(updateInvocation.get(), equalTo(2));
    }

    public static MetaData randomMetaData(IndexTemplateMetaData... templates) {
        MetaData.Builder builder = MetaData.builder();
        for (IndexTemplateMetaData template : templates) {
            builder.put(template);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(
                IndexMetaData.builder(randomAlphaOfLength(10))
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
        }
        return builder.build();
    }

    List<String> randomIndexPatterns() {
        return IntStream.range(0, between(1, 10))
            .mapToObj(n -> randomUnicodeOfCodepointLengthBetween(1, 100))
            .collect(Collectors.toList());
    }
}
