/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;
import org.elasticsearch.xpack.enrich.EnrichPolicyExecutor;
import org.elasticsearch.xpack.enrich.ExecuteEnrichPolicyTask;
import org.junit.Before;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class InternalExecutePolicyActionTests extends ESTestCase {
    /** Only the heavyweight policy runner and transport registration are mocked; execution, scheduling, tasks and spans are real. */
    public void testBackgroundPolicyOwnsScheduledDescendants() throws Exception {
        var pool = new TestThreadPool(getTestName());
        var exporter = InMemorySpanExporter.create();
        var localNode = newNode("local");
        try (
            var clusterService = ClusterServiceUtils.createClusterService(pool, localNode);
            var sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build())
                .build()
        ) {
            var taskManager = new TaskManager(Settings.EMPTY, pool, Set.of(), sdk);
            var service = mock(TransportService.class);
            Mockito.when(service.getThreadPool()).thenReturn(pool);
            Mockito.when(service.getTaskManager()).thenReturn(taskManager);
            var executor = mock(EnrichPolicyExecutor.class);
            var pending = new AtomicReference<Runnable>();
            var policyTask = new AtomicReference<ExecuteEnrichPolicyTask>();
            Mockito.doAnswer(invocation -> {
                ExecuteEnrichPolicyTask task = invocation.getArgument(1);
                ActionListener<ExecuteEnrichPolicyStatus> listener = invocation.getArgument(4);
                policyTask.set(task);
                assertEquals(Span.fromContext(task.getTraceContext()).getSpanContext(), Span.current().getSpanContext());
                pending.set(pool.getThreadContext().preserveContext(() -> {
                    sdk.getTracer("policy-worker").spanBuilder("policy-step").startSpan().end();
                    listener.onResponse(new ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus.PolicyPhases.COMPLETE));
                }));
                return null;
            })
                .when(executor)
                .runPolicyLocally(
                    ArgumentMatchers.any(),
                    ArgumentMatchers.any(),
                    ArgumentMatchers.anyString(),
                    ArgumentMatchers.anyString(),
                    ArgumentMatchers.any()
                );
            var action = new InternalExecutePolicyAction.Transport(
                service,
                ActionFilters.EMPTY,
                clusterService,
                TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                executor
            );
            var parent = sdk.getTracer("test").spanBuilder("submit").startSpan();
            var request = new InternalExecutePolicyAction.Request(TimeValue.THIRTY_SECONDS, "policy", "enrich-index");
            request.setWaitForCompletion(false);
            var initial = new PlainActionFuture<ExecuteEnrichPolicyAction.Response>();
            try (var scope = parent.makeCurrent()) {
                action.doExecute(null, request, ActionListener.wrap(response -> {
                    assertEquals(parent.getSpanContext(), Span.current().getSpanContext());
                    initial.onResponse(response);
                }, initial::onFailure));
                assertEquals(parent.getSpanContext(), Span.current().getSpanContext());
            }
            initial.actionGet();
            parent.end();
            assertTrue(Span.fromContext(policyTask.get().getTraceContext()).isRecording());
            var completed = new PlainActionFuture<Void>();
            pool.generic().execute(() -> {
                pending.get().run();
                assertFalse(Span.current().getSpanContext().isValid());
                completed.onResponse(null);
            });
            completed.actionGet(10, TimeUnit.SECONDS);
            assertTrue(taskManager.getTasks().isEmpty());
            var spans = exporter.getFinishedSpanItems();
            var policy = spans.stream().filter(span -> span.getName().equals(policyTask.get().getAction())).findFirst().orElseThrow();
            var step = spans.stream().filter(span -> span.getName().equals("policy-step")).findFirst().orElseThrow();
            assertEquals(parent.getSpanContext().getSpanId(), policy.getParentSpanId());
            assertEquals(policy.getSpanId(), step.getParentSpanId());
            assertEquals(3, spans.size());
        } finally {
            terminate(pool);
        }
    }

    private InternalExecutePolicyAction.Transport transportAction;

    @Before
    public void instantiateTransportAction() {
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        transportAction = new InternalExecutePolicyAction.Transport(
            transportService,
            mock(ActionFilters.class),
            null,
            TestProjectResolvers.alwaysThrow(),
            null
        );
    }

    public void testSelectNodeForPolicyExecution() {
        var node1 = newNode(randomAlphaOfLength(4));
        var node2 = newNode(randomAlphaOfLength(4));
        var node3 = newNode(randomAlphaOfLength(4));
        var discoNodes = DiscoveryNodes.builder()
            .add(node1)
            .add(node2)
            .add(node3)
            .masterNodeId(node1.getId())
            .localNodeId(node1.getId())
            .build();
        var result = transportAction.selectNodeForPolicyExecution(discoNodes);
        assertThat(result, either(equalTo(node2)).or(equalTo(node3)));
    }

    public void testSelectNodeForPolicyExecutionSingleNode() {
        var node1 = newNode(randomAlphaOfLength(4));
        var discoNodes = DiscoveryNodes.builder().add(node1).masterNodeId(node1.getId()).localNodeId(node1.getId()).build();
        var result = transportAction.selectNodeForPolicyExecution(discoNodes);
        assertThat(result, equalTo(node1));
    }

    public void testSelectNodeForPolicyExecutionDedicatedMasters() {
        var roles = Set.of(DiscoveryNodeRole.MASTER_ROLE);
        var node1 = newNode(randomAlphaOfLength(4), roles);
        var node2 = newNode(randomAlphaOfLength(4), roles);
        var node3 = newNode(randomAlphaOfLength(4), roles);
        var node4 = newNode(randomAlphaOfLength(4));
        var node5 = newNode(randomAlphaOfLength(4));
        var node6 = newNode(randomAlphaOfLength(4));
        var discoNodes = DiscoveryNodes.builder()
            .add(node1)
            .add(node2)
            .add(node3)
            .add(node4)
            .add(node5)
            .add(node6)
            .masterNodeId(node2.getId())
            .localNodeId(node2.getId())
            .build();
        var result = transportAction.selectNodeForPolicyExecution(discoNodes);
        assertThat(result, either(equalTo(node4)).or(equalTo(node5)).or(equalTo(node6)));
    }

    public void testSelectNodeForPolicyExecutionNoNodeWithIngestRole() {
        var roles = Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE);
        var node1 = newNode(randomAlphaOfLength(4), roles);
        var node2 = newNode(randomAlphaOfLength(4), roles);
        var node3 = newNode(randomAlphaOfLength(4), roles);
        var discoNodes = DiscoveryNodes.builder()
            .add(node1)
            .add(node2)
            .add(node3)
            .masterNodeId(node1.getId())
            .localNodeId(node1.getId())
            .build();
        var e = expectThrows(IllegalStateException.class, () -> transportAction.selectNodeForPolicyExecution(discoNodes));
        assertThat(e.getMessage(), equalTo("no ingest nodes in this cluster"));
    }

    public void testSelectNodeForPolicyExecutionPickLocalNodeIfNotElectedMaster() {
        var node1 = newNode(randomAlphaOfLength(4));
        var node2 = newNode(randomAlphaOfLength(4));
        var node3 = newNode(randomAlphaOfLength(4));
        var discoNodes = DiscoveryNodes.builder()
            .add(node1)
            .add(node2)
            .add(node3)
            .masterNodeId(node1.getId())
            .localNodeId(node2.getId())
            .build();
        var result = transportAction.selectNodeForPolicyExecution(discoNodes);
        assertThat(result, equalTo(node2));
    }

    private static DiscoveryNode newNode(String nodeId) {
        return newNode(nodeId, Version.V_7_15_0);
    }

    private static DiscoveryNode newNode(String nodeId, Version version) {
        var roles = Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.INGEST_ROLE);
        return newNode(nodeId, roles, version);
    }

    private static DiscoveryNode newNode(String nodeId, Set<DiscoveryNodeRole> roles) {
        return newNode(nodeId, roles, Version.V_7_15_0);
    }

    private static DiscoveryNode newNode(String nodeId, Set<DiscoveryNodeRole> roles, Version version) {
        return DiscoveryNodeUtils.builder(nodeId).roles(roles).version(version).build();
    }
}
