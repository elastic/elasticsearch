/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.SnapshotEncryptedData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceUnavailableException;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotEncryptedDataFilterTests extends ESTestCase {

    private static final EnumSet<Metadata.XContentContext> SNAPSHOT_CONTEXT = EnumSet.of(
        Metadata.XContentContext.GATEWAY,
        Metadata.XContentContext.SNAPSHOT
    );

    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    public void testNoWarningWhenNoEncryptedData() {
        assertNoWarning(stateWithoutCustom(), new TestHandler(), plainRequest());
    }

    public void testWarningEmittedWhenEncryptedDataPresent() {
        assertWarningEmitted(stateWithCustom(SNAPSHOT_CONTEXT), new TestHandler(), plainRequest());
    }

    public void testWarningEmittedWhenHandlerNotIncludedInSnapshots() {
        assertWarningEmitted(stateWithCustom(SNAPSHOT_CONTEXT), new TestHandler(false, true), requestWithEncryptedData());
    }

    public void testWarningEmittedWhenCustomNotWrittenToSnapshots() {
        assertWarningEmitted(stateWithCustom(EnumSet.of(Metadata.XContentContext.GATEWAY)), new TestHandler(), requestWithEncryptedData());
    }

    public void testNoWarningWhenEncryptedDataSupplied() {
        assumeTrue("only relevant when snapshot encryption feature is enabled", SnapshotEncryptedData.FEATURE_FLAG.isEnabled());
        assertNoWarning(stateWithCustom(SNAPSHOT_CONTEXT), new TestHandler(), requestWithEncryptedData());
    }

    public void testNoWarningWhenIncludeGlobalStateFalse() {
        assertNoWarning(stateWithCustom(SNAPSHOT_CONTEXT), new TestHandler(), plainRequest().includeGlobalState(false));
    }

    public void testNoWarningWhenHandlerReportsNoData() {
        assertNoWarning(stateWithCustom(SNAPSHOT_CONTEXT), new TestHandler(true, false), plainRequest());
    }

    public void testRejectsEncryptedDataWhenEncryptionUnavailable() {
        assumeTrue("only relevant when snapshot encryption feature is enabled", SnapshotEncryptedData.FEATURE_FLAG.isEnabled());
        final EncryptionServiceState unavailable = randomUnavailableState();
        final var filter = filter(stateWithCustom(SNAPSHOT_CONTEXT), new TestHandler(), unavailable);
        final AtomicBoolean proceeded = new AtomicBoolean();
        final EncryptionServiceUnavailableException e = safeAwaitFailure(
            EncryptionServiceUnavailableException.class,
            ActionResponse.class,
            l -> filter.apply(mock(Task.class), "action", requestWithEncryptedData(), l, chain(proceeded))
        );
        assertThat(e.serviceState(), equalTo(unavailable));
        assertFalse("chain.proceed must not be called", proceeded.get());
    }

    public void testNoRejectionWhenEncryptionUnavailableWithoutEncryptedData() {
        assertWarningEmitted(stateWithCustom(SNAPSHOT_CONTEXT), new TestHandler(), plainRequest(), randomUnavailableState());
    }

    public void testNoRejectionWhenEncryptionUnavailableAndNothingToRewrap() {
        assertNoWarning(stateWithoutCustom(), new TestHandler(), requestWithEncryptedData(), randomUnavailableState());
        assertWarningEmitted(
            stateWithCustom(SNAPSHOT_CONTEXT),
            new TestHandler(false, true),
            requestWithEncryptedData(),
            randomUnavailableState()
        );
    }

    private static EncryptionServiceState randomUnavailableState() {
        return randomValueOtherThan(EncryptionServiceState.READY, () -> randomFrom(EncryptionServiceState.values()));
    }

    private static void assertWarningEmitted(ClusterState state, TestHandler handler, CreateSnapshotRequest request) {
        assertWarningEmitted(state, handler, request, EncryptionServiceState.READY);
    }

    private static void assertWarningEmitted(
        ClusterState state,
        TestHandler handler,
        CreateSnapshotRequest request,
        EncryptionServiceState encryptionServiceState
    ) {
        final ThreadContext threadContext = applyFilter(
            state,
            handler,
            encryptionServiceState,
            request,
            new MockLog.SeenEventExpectation(
                "warning logged",
                SnapshotEncryptedDataFilter.class.getName(),
                Level.WARN,
                "*Encrypted credentials*"
            )
        );
        final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        assertThat(responseHeaders, hasKey("Warning"));
        assertThat(responseHeaders.get("Warning").get(0), containsString("Encrypted credentials"));
    }

    private static void assertNoWarning(ClusterState state, TestHandler handler, CreateSnapshotRequest request) {
        assertNoWarning(state, handler, request, EncryptionServiceState.READY);
    }

    private static void assertNoWarning(
        ClusterState state,
        TestHandler handler,
        CreateSnapshotRequest request,
        EncryptionServiceState encryptionServiceState
    ) {
        final ThreadContext threadContext = applyFilter(
            state,
            handler,
            encryptionServiceState,
            request,
            new MockLog.UnseenEventExpectation("no warning", SnapshotEncryptedDataFilter.class.getName(), Level.WARN, "*")
        );
        assertThat(threadContext.getResponseHeaders(), not(hasKey("Warning")));
    }

    private static SnapshotEncryptedDataFilter filter(
        ClusterState state,
        TestHandler handler,
        EncryptionServiceState encryptionServiceState
    ) {
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        return new SnapshotEncryptedDataFilter(
            clusterService,
            DefaultProjectResolver.INSTANCE,
            new EncryptedDataHandlerRegistry(List.of(handler)),
            () -> encryptionServiceState
        );
    }

    private static ThreadContext applyFilter(
        ClusterState state,
        TestHandler handler,
        EncryptionServiceState encryptionServiceState,
        CreateSnapshotRequest request,
        MockLog.LoggingExpectation expectation
    ) {
        final var filter = filter(state, handler, encryptionServiceState);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        try {
            final AtomicBoolean proceeded = new AtomicBoolean();
            MockLog.assertThatLogger(
                () -> filter.apply(mock(Task.class), "action", request, ActionListener.noop(), chain(proceeded)),
                SnapshotEncryptedDataFilter.class,
                expectation
            );
            assertTrue("chain.proceed was not called", proceeded.get());
            return threadContext;
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    private static ClusterState stateWithoutCustom() {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).build();
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(project).build()).build();
    }

    private static ClusterState stateWithCustom(EnumSet<Metadata.XContentContext> context) {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).putCustom(TestCustom.TYPE, new TestCustom(context)).build();
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(project).build()).build();
    }

    private static CreateSnapshotRequest plainRequest() {
        return new CreateSnapshotRequest(TimeValue.ZERO, "repo", "snap");
    }

    private static CreateSnapshotRequest requestWithEncryptedData() {
        return plainRequest().encryptedData(
            new SnapshotEncryptedData(
                SnapshotEncryptedData.TYPE_PASSWORD,
                new SecureString("correct-horse-battery-staple".toCharArray()),
                null
            )
        );
    }

    @SuppressWarnings("unchecked")
    private static <Request extends ActionRequest, Response extends ActionResponse> ActionFilterChain<Request, Response> chain(
        AtomicBoolean proceeded
    ) {
        return (task, action, request, listener) -> proceeded.set(true);
    }

    private record TestHandler(boolean includedInSnapshots, boolean hasData) implements EncryptedDataHandler<TestCustom> {

        TestHandler() {
            this(true, true);
        }

        @Override
        public String customName() {
            return TestCustom.TYPE;
        }

        @Override
        public boolean hasData(TestCustom current) {
            return hasData && current != null;
        }

        @Override
        public TestCustom reEncrypt(TestCustom current, UnaryOperator<EncryptedData> rewrapper) {
            return current;
        }
    }

    private static final class TestCustom extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

        static final String TYPE = "test_encrypted_custom_for_snapshot_warning";

        private final EnumSet<Metadata.XContentContext> context;

        TestCustom(EnumSet<Metadata.XContentContext> context) {
            this.context = context;
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return context;
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Collections.emptyIterator();
        }
    }
}
