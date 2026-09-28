/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.TracingContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.EmptyRequest;
import org.elasticsearch.transport.FakeTcpChannel;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TestTransportChannels;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Contract tests use ordinary native API calls, including instrumentation that has no relationship to tasks or actions. */
public class NativeTracingTests extends ESTestCase {
    public void testInvalidSpanNamesRetainSdkBehaviorWithFilters() {
        try (var fixture = new NativeTracingFixture(Settings.builder().putList("telemetry.tracing.names.exclude", "excluded").build())) {
            var tracer = fixture.api.getTracer("component");
            for (String name : new String[] { null, "", " " }) {
                tracer.spanBuilder(name).startSpan().end();
            }
            assertEquals(3, fixture.exporter.getFinishedSpanItems().size());
            for (var span : fixture.exporter.getFinishedSpanItems()) {
                assertEquals("<unspecified span name>", span.getName());
            }
        }
    }

    public void testDispatchSnapshotSupportsCompatibleTransportVersions() throws Exception {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var context = new ThreadContext(Settings.EMPTY);
            var parent = fixture.api.getTracer("component").spanBuilder("dispatch").startSpan();
            org.elasticsearch.common.io.stream.Writeable snapshot;
            try (var scope = parent.makeCurrent()) {
                snapshot = context.captureAsWriteable();
            }
            var unrelated = fixture.api.getTracer("component").spanBuilder("serialize").startSpan();
            try (var scope = unrelated.makeCurrent()) {
                for (var version : List.of(TransportVersion.minimumCompatible(), TransportVersion.current())) {
                    try (var output = new org.elasticsearch.common.io.stream.BytesStreamOutput()) {
                        output.setTransportVersion(version);
                        snapshot.writeTo(output);
                        try (var input = output.bytes().streamInput()) {
                            input.setTransportVersion(version);
                            var received = new ThreadContext(Settings.EMPTY);
                            received.readHeaders(input);
                            var extracted = Span.fromContext(TracingContext.extract(received)).getSpanContext();
                            assertEquals(parent.getSpanContext().getTraceId(), extracted.getTraceId());
                            assertEquals(parent.getSpanContext().getSpanId(), extracted.getSpanId());
                            assertEquals(parent.getSpanContext().getTraceFlags(), extracted.getTraceFlags());
                            assertTrue(extracted.isRemote());
                            assertEquals(-1, input.read());
                        }
                    }
                }
            } finally {
                unrelated.end();
                parent.end();
            }
        }
    }

    public void testTaskPreservesNonSpanContextWhenTracingIsAbsent() {
        var pool = new TestThreadPool(getTestName());
        try {
            var manager = new TaskManager(Settings.EMPTY, pool, Set.of());
            var key = ContextKey.<String>named("component.metadata");
            try (var parent = Context.root().with(key, "value").makeCurrent()) {
                for (boolean trace : List.of(true, false)) {
                    var task = manager.register("test", "task", new EmptyRequest(), trace);
                    try (var scope = manager.withTaskContext(task)) {
                        assertEquals("value", Context.current().get(key));
                        assertFalse(Span.current().getSpanContext().isValid());
                    } finally {
                        manager.unregister(task);
                    }
                }
            }
        } finally {
            terminate(pool);
        }
    }

    /**
     * A banned parent makes registration unregister the task before {@code register} even returns. The span must
     * already exist at that point, otherwise it would never be ended, and it must be ended exactly once.
     */
    public void testRegistrationRacingUnregisterStillEndsTheSpanExactlyOnce() {
        var threadPool = new TestThreadPool(getTestName());
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var manager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), fixture.api);
            var parentTaskId = new TaskId("other-node", 42);
            manager.setBan(
                parentTaskId,
                "banned for test",
                TestTransportChannels.newFakeTcpTransportChannel(
                    "test",
                    new FakeTcpChannel(),
                    threadPool,
                    "banned",
                    randomNonNegativeLong(),
                    TransportVersion.current()
                )
            );

            var request = new CancellableRequest();
            request.setParentTask(parentTaskId);
            var parent = fixture.api.getTracer("client").spanBuilder("request").startSpan();
            try (var scope = parent.makeCurrent()) {
                expectThrows(TaskCancelledException.class, () -> manager.register("transport", "banned", request));
            }
            parent.end();

            var banned = fixture.span("banned");
            assertEquals(parent.getSpanContext().getSpanId(), banned.getParentSpanId());
            assertEquals(StatusCode.UNSET, banned.getStatus().getStatusCode());
            assertTrue(manager.getTasks().isEmpty());
            assertEquals(2, fixture.exporter.getFinishedSpanItems().size());
        } finally {
            terminate(threadPool);
        }
    }

    /** Cleanup can be attempted more than once for the same task; only the first attempt may end the span. */
    public void testDuplicateUnregisterEndsTheTaskSpanOnce() {
        var threadPool = new TestThreadPool(getTestName());
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var manager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), fixture.api);
            var parent = fixture.api.getTracer("client").spanBuilder("request").startSpan();
            Task task;
            try (var scope = parent.makeCurrent()) {
                task = manager.register("transport", "repeated", new EmptyRequest());
            }
            assertTrue(Span.fromContext(task.getTraceContext()).isRecording());
            manager.unregister(task);
            manager.unregister(task);
            task.finishTrace();
            parent.end();

            assertEquals(2, fixture.exporter.getFinishedSpanItems().size());
            assertEquals(parent.getSpanContext().getSpanId(), fixture.span("repeated").getParentSpanId());
        } finally {
            terminate(threadPool);
        }
    }

    public void testCachedNativeTracerCannotResurrectClosedService() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var tracer = fixture.api.getTracer("component");
            assertTrue(tracer.isEnabled());
            fixture.service.close();
            fixture.service.setEnabled(true);
            assertFalse(tracer.isEnabled());
            assertFalse(tracer.spanBuilder("late").startSpan().isRecording());
        }
    }

    public void testTaskAttributesUseConfiguredRedaction() {
        var pool = new TestThreadPool(getTestName());
        try (
            var fixture = new NativeTracingFixture(
                Settings.builder().putList("telemetry.tracing.sanitize_field_names", "es.task.*").build()
            )
        ) {
            var manager = new TaskManager(Settings.EMPTY, pool, Set.of(), fixture.api);
            var parent = fixture.api.getTracer("test").spanBuilder("parent").startSpan();
            try (var scope = parent.makeCurrent()) {
                for (boolean redact : List.of(true, false)) {
                    if (redact == false) {
                        fixture.service.setLabelFilters(List.of("password"));
                    }
                    var request = new EmptyRequest();
                    request.setParentTask(new TaskId("parent-node", 42));
                    var task = manager.register("transport", "task-" + redact, request);
                    manager.unregister(task);
                    var attributes = fixture.span("task-" + redact).getAttributes();
                    if (redact) {
                        assertEquals("[REDACTED]", attributes.get(AttributeKey.stringKey("es.task.id")));
                        assertEquals("[REDACTED]", attributes.get(AttributeKey.stringKey("es.task.parent.id")));
                    } else {
                        assertEquals(Long.valueOf(task.getId()), attributes.get(AttributeKey.longKey("es.task.id")));
                        assertEquals("parent-node:42", attributes.get(AttributeKey.stringKey("es.task.parent.id")));
                    }
                }
            } finally {
                parent.end();
            }
        } finally {
            terminate(pool);
        }
    }

    public void testRejectionAndCleanupRestoreCapturedContext() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var context = new ThreadContext(Settings.EMPTY);
            var parent = fixture.api.getTracer("test").spanBuilder("parent").startSpan();
            org.elasticsearch.common.util.concurrent.AbstractRunnable runnable;
            var invoked = new java.util.concurrent.atomic.AtomicInteger();
            try (var scope = parent.makeCurrent()) {
                runnable = asInstanceOf(
                    org.elasticsearch.common.util.concurrent.AbstractRunnable.class,
                    context.preserveContext(new org.elasticsearch.common.util.concurrent.AbstractRunnable() {
                        @Override
                        protected void doRun() {
                            fail("must not execute");
                        }

                        @Override
                        public void onFailure(Exception failure) {
                            throw new AssertionError(failure);
                        }

                        @Override
                        public void onRejection(Exception failure) {
                            assertEquals(parent.getSpanContext(), Span.current().getSpanContext());
                            invoked.incrementAndGet();
                        }

                        @Override
                        public void onAfter() {
                            assertEquals(parent.getSpanContext(), Span.current().getSpanContext());
                            invoked.incrementAndGet();
                        }
                    })
                );
            }
            runnable.onRejection(new org.elasticsearch.common.util.concurrent.EsRejectedExecutionException("rejected"));
            assertFalse(Span.current().getSpanContext().isValid());
            runnable.onAfter();
            assertFalse(Span.current().getSpanContext().isValid());
            assertEquals(2, invoked.get());
            parent.end();
        }
    }

    public void testNativeContextAcrossTcpProxy() throws Exception {
        assertTcpProxy(false);
    }

    public void testTransportedCancellationAcrossTcpProxy() throws Exception {
        assertTcpProxy(true);
    }

    private void assertTcpProxy(boolean cancel) throws Exception {
        var pool = new TestThreadPool(getTestName());
        try (
            var senderFixture = new NativeTracingFixture(Settings.EMPTY);
            var proxyFixture = new NativeTracingFixture(Settings.EMPTY);
            var targetFixture = new NativeTracingFixture(Settings.EMPTY);
            var sender = transport("sender", pool, senderFixture);
            var proxy = transport("proxy", pool, proxyFixture);
            var target = transport("target", pool, targetFixture)
        ) {
            String action = "internal:tracing/test";
            proxy.registerRequestHandler(action, pool.generic(), EmptyRequest::new, (request, channel, task) -> {
                throw new AssertionError("the proxy must forward this action");
            });
            target.registerRequestHandler(action, pool.generic(), EmptyRequest::new, (request, channel, task) -> {
                assertEquals(Span.fromContext(task.getTraceContext()).getSpanContext(), Span.current().getSpanContext());
                targetFixture.api.getTracer("ordinary-component").spanBuilder("dependency").startSpan().end();
                if (cancel) {
                    channel.sendResponse(new TaskCancelledException("cancelled remotely"));
                } else {
                    channel.sendResponse(ActionResponse.Empty.INSTANCE);
                }
            });
            TransportActionProxy.registerProxyAction(
                proxy,
                action,
                false,
                input -> ActionResponse.Empty.INSTANCE,
                new NamedWriteableRegistry(List.of())
            );
            sender.start();
            proxy.start();
            target.start();
            sender.acceptIncomingRequests();
            proxy.acceptIncomingRequests();
            target.acceptIncomingRequests();
            try (
                Releasable firstConnection = safeAwait(listener -> sender.connectToNode(proxy.getLocalNode(), listener));
                Releasable secondConnection = safeAwait(listener -> proxy.connectToNode(target.getLocalNode(), listener))
            ) {
                var future = new PlainActionFuture<ActionResponse.Empty>();
                var parent = senderFixture.api.getTracer("client").spanBuilder("request").startSpan();
                String proxyAction = TransportActionProxy.getProxyAction(action);
                try (var scope = parent.makeCurrent()) {
                    sender.sendRequest(
                        proxy.getLocalNode(),
                        proxyAction,
                        TransportActionProxy.wrapRequest(target.getLocalNode(), new EmptyRequest()),
                        new ActionListenerResponseHandler<>(
                            future,
                            input -> ActionResponse.Empty.INSTANCE,
                            TransportResponseHandler.TRANSPORT_WORKER
                        )
                    );
                }
                if (cancel) {
                    var failure = expectThrows(Exception.class, () -> future.actionGet(10, TimeUnit.SECONDS));
                    assertTrue(ExceptionsHelper.unwrapCause(failure) instanceof TaskCancelledException);
                } else {
                    future.actionGet(10, TimeUnit.SECONDS);
                }
                parent.end();
                assertBusy(() -> {
                    var forwarded = proxyFixture.span(proxyAction);
                    var received = targetFixture.span(action);
                    assertEquals(parent.getSpanContext().getSpanId(), forwarded.getParentSpanId());
                    assertEquals(forwarded.getSpanId(), received.getParentSpanId());
                    assertEquals(received.getSpanId(), targetFixture.span("dependency").getParentSpanId());
                    assertEquals(parent.getSpanContext().getTraceId(), received.getTraceId());
                    for (var span : List.of(forwarded, received)) {
                        // Task spans report no outcome of their own, whether the call succeeded or was cancelled.
                        assertEquals(StatusCode.UNSET, span.getStatus().getStatusCode());
                        assertNull(span.getAttributes().get(AttributeKey.stringKey("es.outcome")));
                        assertNull(span.getAttributes().get(AttributeKey.stringKey("error.type")));
                    }
                    assertTrue(proxy.getTaskManager().getTasks().isEmpty());
                    assertTrue(target.getTaskManager().getTasks().isEmpty());
                });
            }
        } finally {
            terminate(pool);
        }
    }

    private MockTransportService transport(String name, TestThreadPool pool, NativeTracingFixture fixture) {
        var settings = Settings.builder().put("node.name", name).put("tests.mock.taskmanager.enabled", randomBoolean()).build();
        return new MockTransportService(
            settings,
            MockTransportService.newMockTransport(settings, TransportVersion.current(), pool),
            pool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            address -> DiscoveryNodeUtils.builder(name).name(name).address(address.publishAddress()).build(),
            null,
            Set.of(),
            name,
            fixture.api
        );
    }

    public void testOrdinaryNativeScopesPreserveDepthAndMetadata() {
        try (var fixture = new NativeTracingFixture(Settings.builder().put("telemetry.tracing.max_depth", 1).build())) {
            var tracer = fixture.api.tracerBuilder("component")
                .setInstrumentationVersion("1")
                .setSchemaUrl("https://example.test/schema")
                .build();
            var entry = tracer.spanBuilder("entry").startSpan();
            var metadata = ContextKey.<String>named("test.metadata");
            try (var scope = Context.root().with(metadata, "preserved").with(entry).makeCurrent()) {
                var phase = tracer.spanBuilder("phase").startSpan();
                try (var phaseScope = phase.makeCurrent()) {
                    assertEquals("preserved", Context.current().get(metadata));
                    var suppressed = tracer.spanBuilder("too-deep").startSpan();
                    assertFalse(suppressed.isRecording());
                    assertEquals(phase.getSpanContext(), suppressed.getSpanContext());
                    suppressed.end();
                    assertTrue(phase.isRecording());
                }
                assertEquals(entry.getSpanContext(), Span.current().getSpanContext());
                phase.end();
            }
            entry.end();
            assertFalse(Span.current().getSpanContext().isValid());
            assertEquals(2, fixture.exporter.getFinishedSpanItems().size());
            var phase = fixture.span("phase");
            assertEquals(entry.getSpanContext().getSpanId(), phase.getParentSpanId());
            assertEquals("component", phase.getInstrumentationScopeInfo().getName());
            assertEquals("1", phase.getInstrumentationScopeInfo().getVersion());
            assertEquals("https://example.test/schema", phase.getInstrumentationScopeInfo().getSchemaUrl());
        }
    }

    public void testNativeTracerBuilderWithoutOptionalMetadata() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            fixture.api.getTracerProvider().tracerBuilder("component").build().spanBuilder("operation").startSpan().end();
            assertEquals("component", fixture.span("operation").getInstrumentationScopeInfo().getName());
        }
    }

    public void testExcludedSpansDoNotHideDescendantsOrMutateParents() {
        try (var fixture = new NativeTracingFixture(Settings.builder().putList("telemetry.tracing.names.exclude", "excluded").build())) {
            var tracer = fixture.api.getTracer("component");
            var parent = tracer.spanBuilder("parent").startSpan();
            try (var scope = parent.makeCurrent()) {
                var excluded = tracer.spanBuilder("excluded").startSpan();
                try (var excludedScope = excluded.makeCurrent()) {
                    TracingContext.recordFailure(excluded, new IllegalArgumentException("ignored"));
                    tracer.spanBuilder("allowed").startSpan().end();
                }
                excluded.end();
                assertTrue(parent.isRecording());
            }
            parent.end();
            assertEquals(parent.getSpanContext().getSpanId(), fixture.span("allowed").getParentSpanId());
            assertEquals(StatusCode.UNSET, fixture.span("parent").getStatus().getStatusCode());
            assertEquals(2, fixture.exporter.getFinishedSpanItems().size());
        }
    }

    public void testCachedTracerObservesLiveRecordingPolicy() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var tracer = fixture.api.getTracer("component");
            var parent = tracer.spanBuilder("parent").startSpan();
            try (var scope = parent.makeCurrent()) {
                fixture.service.setEnabled(false);
                var disabled = tracer.spanBuilder("disabled").startSpan();
                assertFalse(disabled.isRecording());
                assertEquals(parent.getSpanContext(), disabled.getSpanContext());
                disabled.end();
                fixture.service.setEnabled(true);
                fixture.service.setIncludeNames(List.of("allowed*"));
                assertFalse(tracer.spanBuilder("other").startSpan().isRecording());
                tracer.spanBuilder("allowed-child").startSpan().end();
                fixture.service.setMaxTraceDepth(0);
                assertFalse(tracer.spanBuilder("allowed-too-deep").startSpan().isRecording());
            }
            parent.end();
            assertEquals(2, fixture.exporter.getFinishedSpanItems().size());
            assertEquals(parent.getSpanContext().getSpanId(), fixture.span("allowed-child").getParentSpanId());
        }
    }

    public void testLocalSuppressionPreservesRemoteSampling() {
        try (
            var local = new NativeTracingFixture(Settings.builder().put("telemetry.tracing.max_depth", 0).build());
            var remote = new NativeTracingFixture(Settings.builder().put("telemetry.tracing.max_depth", 0).build())
        ) {
            var root = local.api.getTracer("component").spanBuilder("root").startSpan();
            var wire = new ThreadContext(Settings.EMPTY);
            try (var scope = root.makeCurrent()) {
                var suppressed = local.api.getTracer("component").spanBuilder("local-child").startSpan();
                try (var childScope = suppressed.makeCurrent()) {
                    wire.putHeader(TracingContext.headers(Map.of()));
                }
                suppressed.end();
            }
            var extracted = TracingContext.extract(wire);
            assertTrue(Span.fromContext(extracted).getSpanContext().isSampled());
            var entry = remote.api.getTracer("component").spanBuilder("remote-entry").setParent(extracted).startSpan();
            assertTrue(entry.isRecording());
            entry.end();
            root.end();
            assertEquals(root.getSpanContext().getSpanId(), remote.span("remote-entry").getParentSpanId());
        }
    }

    public void testUnsampledParentStillPropagates() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY, Sampler.parentBased(Sampler.alwaysOff()))) {
            var span = fixture.api.getTracer("component").spanBuilder("unsampled").startSpan();
            assertTrue(span.getSpanContext().isValid());
            assertFalse(span.getSpanContext().isSampled());
            try (var scope = span.makeCurrent()) {
                var wire = new ThreadContext(Settings.EMPTY);
                wire.putHeader(TracingContext.headers(Map.of()));
                assertEquals(
                    span.getSpanContext().getTraceId(),
                    Span.fromContext(TracingContext.extract(wire)).getSpanContext().getTraceId()
                );
                assertFalse(Span.fromContext(TracingContext.extract(wire)).getSpanContext().isSampled());
            }
            span.end();
            assertTrue(fixture.exporter.getFinishedSpanItems().isEmpty());
        }
    }

    public void testNativeContextSurvivesCallbacksWithoutLeaking() throws Exception {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY); var executor = Executors.newSingleThreadExecutor()) {
            var threadContext = new ThreadContext(Settings.EMPTY);
            for (String name : List.of("first", "second")) {
                var parent = fixture.api.getTracer("component").spanBuilder(name).startSpan();
                Runnable callback;
                try (var scope = parent.makeCurrent()) {
                    callback = threadContext.preserveContext(() -> {
                        assertEquals(parent.getSpanContext(), Span.current().getSpanContext());
                        assertEquals(parent.getSpanContext().getTraceId(), threadContext.getHeader(Task.TRACE_ID));
                        fixture.api.getTracer("component").spanBuilder(name + "-callback").startSpan().end();
                    });
                }
                executor.submit(() -> {
                    callback.run();
                    assertFalse(Span.current().getSpanContext().isValid());
                    assertTrue(threadContext.isDefaultContext());
                }).get(10, TimeUnit.SECONDS);
                parent.end();
                assertEquals(parent.getSpanContext().getSpanId(), fixture.span(name + "-callback").getParentSpanId());
            }
        }
    }

    public void testDelayedSnapshotAndDetachedWork() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var threadContext = new ThreadContext(Settings.EMPTY);
            var parent = fixture.api.getTracer("component").spanBuilder("parent").startSpan();
            ThreadContext.StoredContext snapshot;
            try (var scope = parent.makeCurrent()) {
                snapshot = threadContext.newStoredContext();
            }
            var unrelated = fixture.api.getTracer("component").spanBuilder("unrelated").startSpan();
            try (var scope = unrelated.makeCurrent()) {
                var supplier = threadContext.wrapRestorable(snapshot);
                try (var restored = supplier.get()) {
                    assertEquals(parent.getSpanContext(), Span.current().getSpanContext());
                    for (int variant = 0; variant < 3; variant++) {
                        try (var detached = switch (variant) {
                            case 0 -> threadContext.newEmptyContext();
                            case 1 -> threadContext.newEmptySystemContext();
                            case 2 -> threadContext.clearTraceContext();
                            default -> throw new AssertionError(variant);
                        }) {
                            assertFalse(Span.current().getSpanContext().isValid());
                            assertNull(threadContext.getHeader(Task.TRACE_ID));
                        }
                        assertEquals(parent.getSpanContext(), Span.current().getSpanContext());
                    }
                }
                assertEquals(unrelated.getSpanContext(), Span.current().getSpanContext());
            }
            parent.end();
            unrelated.end();
        }
    }

    public void testAllAttributeSurfacesAreSanitizedAtExport() {
        try (
            var fixture = new NativeTracingFixture(
                Settings.builder().putList("telemetry.tracing.sanitize_field_names", "private.*").build()
            )
        ) {
            var linked = fixture.api.getTracer("component").spanBuilder("linked").startSpan();
            var span = fixture.api.getTracer("component")
                .spanBuilder("redacted")
                .setAttribute("private.id", 42L)
                .addLink(linked.getSpanContext(), Attributes.of(AttributeKey.stringKey("private.link"), "secret"))
                .startSpan();
            span.setAttribute("private.late", "secret");
            span.setAllAttributes(Attributes.builder().put("private.array", "secret", "another").put("public.id", 7L).build());
            span.addEvent("event", Attributes.of(AttributeKey.stringKey("private.event"), "secret"));
            span.addLink(linked.getSpanContext(), Attributes.of(AttributeKey.stringKey("private.late-link"), "secret"));
            span.end();
            linked.end();
            var exported = fixture.span("redacted");
            for (String name : List.of("id", "late", "array")) {
                assertEquals("[REDACTED]", exported.getAttributes().get(AttributeKey.stringKey("private." + name)));
            }
            assertEquals(Long.valueOf(7), exported.getAttributes().get(AttributeKey.longKey("public.id")));
            assertNull(exported.getAttributes().get(AttributeKey.longKey("private.id")));
            assertEquals("[REDACTED]", exported.getEvents().getFirst().getAttributes().get(AttributeKey.stringKey("private.event")));
            assertEquals("[REDACTED]", exported.getLinks().getFirst().getAttributes().get(AttributeKey.stringKey("private.link")));
            assertEquals("[REDACTED]", exported.getLinks().getLast().getAttributes().get(AttributeKey.stringKey("private.late-link")));
        }
    }

    public void testEmptySanitizationListDoesNotRedactAttributes() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            fixture.service.setLabelFilters(List.of());
            fixture.api.getTracer("component").spanBuilder("unredacted").setAttribute("private.value", 42L).startSpan().end();

            var attributes = fixture.span("unredacted").getAttributes();
            assertEquals(Long.valueOf(42), attributes.get(AttributeKey.longKey("private.value")));
            assertNull(attributes.get(AttributeKey.stringKey("private.value")));
        }
    }

    public void testExceptionPolicyAndRedactionRemainLive() {
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var tracer = fixture.api.getTracer("component");
            var first = tracer.spanBuilder("first").startSpan();
            first.recordException(new IllegalArgumentException("message"));
            first.end();
            assertNull(fixture.span("first").getEvents().getFirst().getAttributes().get(AttributeKey.stringKey("exception.stacktrace")));
            fixture.service.setRecordExceptionStacks(true);
            var second = tracer.spanBuilder("second").startSpan();
            second.recordException(new IllegalArgumentException("secret"));
            fixture.service.setLabelFilters(List.of("exception.*"));
            second.end();
            var event = fixture.span("second").getEvents().getFirst();
            assertEquals("[REDACTED]", event.getAttributes().get(AttributeKey.stringKey("exception.message")));
            assertEquals("[REDACTED]", event.getAttributes().get(AttributeKey.stringKey("exception.stacktrace")));
        }
    }

    public void testDeferredTaskCompletionEndsTheSpanUnderItsOriginalParent() {
        var threadPool = new TestThreadPool(getTestName());
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var manager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), fixture.api);
            var completion = new AtomicReference<ActionListener<ActionResponse.Empty>>();
            var response = new PlainActionFuture<ActionResponse.Empty>();
            var parent = fixture.api.getTracer("component").spanBuilder("submit").startSpan();
            Task task;
            try (var scope = parent.makeCurrent()) {
                task = manager.registerAndExecute(
                    "transport",
                    new TransportAction<TestRequest, ActionResponse.Empty>(
                        "background",
                        ActionFilters.EMPTY,
                        manager,
                        EsExecutors.DIRECT_EXECUTOR_SERVICE
                    ) {
                        @Override
                        protected void doExecute(Task executing, TestRequest request, ActionListener<ActionResponse.Empty> listener) {
                            assertEquals(Span.fromContext(executing.getTraceContext()).getSpanContext(), Span.current().getSpanContext());
                            completion.set(listener);
                        }
                    },
                    new TestRequest(),
                    null,
                    response
                );
                assertEquals(parent.getSpanContext(), Span.current().getSpanContext());
            }
            parent.end();
            assertTrue(Span.fromContext(task.getTraceContext()).isRecording());
            completion.get().onFailure(new RemoteTransportException("remote", new TaskCancelledException("cancelled")));
            manager.unregister(task);
            var exported = fixture.span("background");
            // The failure is reported to the caller, not stamped onto the task span.
            assertNull(exported.getAttributes().get(AttributeKey.stringKey("es.outcome")));
            assertNull(exported.getAttributes().get(AttributeKey.stringKey("error.type")));
            assertEquals(StatusCode.UNSET, exported.getStatus().getStatusCode());
            assertEquals(parent.getSpanContext().getSpanId(), exported.getParentSpanId());
            assertTrue(manager.getTasks().isEmpty());
        } finally {
            terminate(threadPool);
        }
    }

    public void testUntracedTasksCannotChangeTheirParent() {
        var threadPool = new TestThreadPool(getTestName());
        try (var fixture = new NativeTracingFixture(Settings.EMPTY)) {
            var manager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), fixture.api);
            var parent = fixture.api.getTracer("component").spanBuilder("parent").startSpan();
            try (var scope = parent.makeCurrent()) {
                var task = manager.register("transport", "untraced", new EmptyRequest(), false);
                try (var activation = manager.withTaskContext(task)) {
                    assertEquals(parent.getSpanContext(), Span.current().getSpanContext());
                    assertFalse(Span.current().isRecording());
                    Span.current().setStatus(StatusCode.ERROR);
                    Span.current().end();
                }
                manager.unregister(task);
                assertTrue(parent.isRecording());
            }
            parent.end();
            assertEquals(1, fixture.exporter.getFinishedSpanItems().size());
            assertEquals(StatusCode.UNSET, fixture.span("parent").getStatus().getStatusCode());
        } finally {
            terminate(threadPool);
        }
    }

    /** Only cancellable tasks take the ban-checking registration path, where unregister runs inside {@code register}. */
    private static class CancellableRequest extends AbstractTransportRequest {
        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }
    }

    /** An action with no payload isolates task lifetime from request validation and serialization. */
    private static class TestRequest extends ActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) {}
    }
}
