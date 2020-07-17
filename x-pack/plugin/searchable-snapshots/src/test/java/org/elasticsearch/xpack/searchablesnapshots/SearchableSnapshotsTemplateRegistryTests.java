/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsTemplateRegistry.INDEX_TEMPLATE_VERSION;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsTemplateRegistry.SNAPSHOTS_CACHE_TEMPLATE_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SearchableSnapshotsTemplateRegistryTests extends ESTestCase {

    private void executeTest(
        final ClusterState.Builder clusterState,
        final BiFunction<ActionType<?>, ActionRequest, ActionResponse> consumer
    ) {
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool)) {
            final NoOpClient client = new NoOpClient(threadPool) {
                @Override
                @SuppressWarnings("unchecked")
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    try {
                        listener.onResponse((Response) consumer.apply(action, request));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }
            };

            SearchableSnapshotsTemplateRegistry.register(Settings.EMPTY, clusterService, threadPool, client, xContentRegistry());
            ClusterServiceUtils.setState(clusterService, clusterState);
        } catch (Exception e) {
            throw new AssertionError("", e);
        } finally {
            terminate(threadPool);
        }
    }

    public void testTemplateDoesNotExist() throws Exception {
        final ClusterState.Builder clusterState = newEmptyClusterState();

        final PlainActionFuture<PutComposableIndexTemplateAction.Request> executed = PlainActionFuture.newFuture();
        executeTest(clusterState, (action, request) -> {
            if (action instanceof PutComposableIndexTemplateAction) {
                assertThat(request, instanceOf(PutComposableIndexTemplateAction.Request.class));
                executed.onResponse((PutComposableIndexTemplateAction.Request) request);
                return new AcknowledgedResponse(true);
            } else {
                executed.onFailure(unsupportedAction(action));
                return new AcknowledgedResponse(false);
            }
        });

        assertComposableIndexTemplateRequest(executed.get());
    }

    public void testTemplateAlreadyExists() {
        final ClusterState.Builder clusterState = newEmptyClusterState();
        clusterState.metadata(
            Metadata.builder()
                .put(
                    ".snapshots",
                    new ComposableIndexTemplate(
                        List.of(".snapshots-0"),
                        new Template(Settings.EMPTY, null, Map.of(".snapshots", AliasMetadata.builder(".snapshots").build())),
                        null,
                        123L,
                        1L,
                        emptyMap()
                    )
                )
                .build()
        );

        final AtomicBoolean executed = new AtomicBoolean(false);
        executeTest(clusterState, (action, request) -> {
            executed.set(true);
            fail("Index template should not be created");
            return null;
        });
        assertThat(executed.get(), is(false));
    }

    public void testTemplateShouldBeUpdated() throws Exception {
        final ClusterState.Builder clusterState = newEmptyClusterState();
        clusterState.metadata(
            Metadata.builder()
                .put(
                    IndexTemplateMetadata.builder(".snapshots")
                        .patterns(List.of(randomAlphaOfLength(10)))
                        .version(randomBoolean() ? 0 : null) // old or non-existent version
                        .putAlias(AliasMetadata.builder(randomAlphaOfLength(10)).build())
                        .build()
                )
                .build()
        );

        final PlainActionFuture<PutComposableIndexTemplateAction.Request> executed = PlainActionFuture.newFuture();
        executeTest(clusterState, (action, request) -> {
            if (action instanceof PutComposableIndexTemplateAction) {
                assertThat(request, instanceOf(PutComposableIndexTemplateAction.Request.class));
                executed.onResponse((PutComposableIndexTemplateAction.Request) request);
                return new AcknowledgedResponse(true);
            } else {
                executed.onFailure(unsupportedAction(action));
                return new AcknowledgedResponse(false);
            }
        });

        assertComposableIndexTemplateRequest(executed.get());
    }

    public void testThatMissingMasterNodeDoesNothing() {
        final ClusterState.Builder clusterState = newEmptyClusterState();
        clusterState.nodes(DiscoveryNodes.builder(clusterState.nodes()).masterNodeId(null));

        final AtomicBoolean executed = new AtomicBoolean(false);
        executeTest(clusterState, (action, request) -> {
            executed.set(true);
            fail("if the master is missing nothing should happen");
            return null;
        });
        assertThat(clusterState.build().nodes().getMasterNode(), nullValue());
        assertThat(executed.get(), is(false));
    }

    private static void assertComposableIndexTemplateRequest(final PutComposableIndexTemplateAction.Request templateRequest) {
        assertThat(templateRequest.name(), equalTo(SNAPSHOTS_CACHE_TEMPLATE_NAME));
        final ComposableIndexTemplate indexTemplate = templateRequest.indexTemplate();
        assertThat(indexTemplate.version(), equalTo((long) INDEX_TEMPLATE_VERSION));
        assertThat(indexTemplate.indexPatterns(), hasItem(equalTo(SNAPSHOTS_CACHE_TEMPLATE_NAME + "-" + INDEX_TEMPLATE_VERSION)));
        assertThat(indexTemplate.indexPatterns(), hasSize(1));
        final List<String> aliases = new ArrayList<>(indexTemplate.template().aliases().keySet());
        assertThat(aliases, hasItem(equalTo(SNAPSHOTS_CACHE_TEMPLATE_NAME)));
        assertThat(aliases, hasSize(1));
    }

    private Exception unsupportedAction(ActionType<?> action) {
        return new UnsupportedOperationException("The test [" + getTestName() + " ] does not support action [" + action.name() + ']');
    }

    private ClusterState.Builder newEmptyClusterState() {
        final DiscoveryNode node = new DiscoveryNode(randomAlphaOfLength(10), buildNewFakeTransportAddress(), Version.CURRENT);
        return ClusterState.builder(new ClusterName(getTestName()))
            .version(0L)
            .nodes(DiscoveryNodes.builder().add(node).localNodeId(node.getId()).masterNodeId(node.getId()));
    }
}
