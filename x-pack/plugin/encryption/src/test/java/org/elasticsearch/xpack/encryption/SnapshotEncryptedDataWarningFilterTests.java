/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotEncryptedDataWarningFilterTests extends ESTestCase {

    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    public void testNoWarningWhenNoEncryptedData() {
        ClusterState state = stateWithoutCustom();
        ClusterService clusterService = mockClusterService(state);
        var registry = new EncryptedDataHandlerRegistry(List.of(handlerFor("test_custom")));
        var filter = new SnapshotEncryptedDataWarningFilter(clusterService, DefaultProjectResolver.INSTANCE, registry);

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        try {
            AtomicBoolean proceeded = new AtomicBoolean();
            MockLog.assertThatLogger(
                () -> filter.apply(
                    mock(Task.class),
                    "action",
                    new CreateSnapshotRequest(TimeValue.ZERO, "repo", "snap"),
                    ActionListener.noop(),
                    chain(proceeded)
                ),
                SnapshotEncryptedDataWarningFilter.class,
                new MockLog.UnseenEventExpectation(
                    "no warning",
                    SnapshotEncryptedDataWarningFilter.class.getName(),
                    org.apache.logging.log4j.Level.WARN,
                    "*"
                )
            );
            assertTrue("chain.proceed was not called", proceeded.get());
            assertThat(threadContext.getResponseHeaders(), not(hasKey("Warning")));
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    public void testWarningEmittedWhenEncryptedDataPresent() {
        TestCustom custom = new TestCustom();
        ClusterState state = stateWithCustom(custom);
        ClusterService clusterService = mockClusterService(state);
        var registry = new EncryptedDataHandlerRegistry(List.of(handlerFor(TestCustom.TYPE)));
        var filter = new SnapshotEncryptedDataWarningFilter(clusterService, DefaultProjectResolver.INSTANCE, registry);

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        try {
            AtomicBoolean proceeded = new AtomicBoolean();
            MockLog.assertThatLogger(
                () -> filter.apply(
                    mock(Task.class),
                    "action",
                    new CreateSnapshotRequest(TimeValue.ZERO, "repo", "snap"),
                    ActionListener.noop(),
                    chain(proceeded)
                ),
                SnapshotEncryptedDataWarningFilter.class,
                new MockLog.SeenEventExpectation(
                    "warning logged",
                    SnapshotEncryptedDataWarningFilter.class.getName(),
                    org.apache.logging.log4j.Level.WARN,
                    "*Encrypted data source credentials*"
                )
            );
            assertTrue("chain.proceed was not called", proceeded.get());
            Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
            assertThat(responseHeaders, hasKey("Warning"));
            assertThat(responseHeaders.get("Warning").get(0), containsString("Encrypted data source credentials"));
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    public void testNoWarningWhenIncludeGlobalStateFalse() {
        TestCustom custom = new TestCustom();
        ClusterState state = stateWithCustom(custom);
        ClusterService clusterService = mockClusterService(state);
        var registry = new EncryptedDataHandlerRegistry(List.of(handlerFor(TestCustom.TYPE)));
        var filter = new SnapshotEncryptedDataWarningFilter(clusterService, DefaultProjectResolver.INSTANCE, registry);

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        try {
            AtomicBoolean proceeded = new AtomicBoolean();
            CreateSnapshotRequest request = new CreateSnapshotRequest(TimeValue.ZERO, "repo", "snap");
            request.includeGlobalState(false);
            MockLog.assertThatLogger(
                () -> filter.apply(mock(Task.class), "action", request, ActionListener.noop(), chain(proceeded)),
                SnapshotEncryptedDataWarningFilter.class,
                new MockLog.UnseenEventExpectation(
                    "no warning",
                    SnapshotEncryptedDataWarningFilter.class.getName(),
                    org.apache.logging.log4j.Level.WARN,
                    "*"
                )
            );
            assertTrue("chain.proceed was not called", proceeded.get());
            assertThat(threadContext.getResponseHeaders(), not(hasKey("Warning")));
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    public void testWarningNotEmittedWhenHandlerCustomAbsentFromState() {
        ClusterState state = stateWithCustom(new TestCustom());
        ClusterService clusterService = mockClusterService(state);
        // Register a handler for a different custom type that is not in the state.
        var registry = new EncryptedDataHandlerRegistry(List.of(handlerFor("other_custom")));
        var filter = new SnapshotEncryptedDataWarningFilter(clusterService, DefaultProjectResolver.INSTANCE, registry);

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        try {
            AtomicBoolean proceeded = new AtomicBoolean();
            filter.apply(
                mock(Task.class),
                "action",
                new CreateSnapshotRequest(TimeValue.ZERO, "repo", "snap"),
                ActionListener.noop(),
                chain(proceeded)
            );
            assertTrue("chain.proceed was not called", proceeded.get());
            assertThat(threadContext.getResponseHeaders(), not(hasKey("Warning")));
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    private static ClusterState stateWithoutCustom() {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).build();
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(project).build()).build();
    }

    private static ClusterState stateWithCustom(TestCustom custom) {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).putCustom(TestCustom.TYPE, custom).build();
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(project).build()).build();
    }

    private static ClusterService mockClusterService(ClusterState state) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        return clusterService;
    }

    private static EncryptedDataHandler<TestCustom> handlerFor(String customName) {
        return new EncryptedDataHandler<>() {
            @Override
            public String customName() {
                return customName;
            }

            @Override
            public TestCustom reEncrypt(TestCustom current, EncryptionService encryptionService, String activeKeyId) {
                return current;
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <Request extends ActionRequest, Response extends ActionResponse> ActionFilterChain<Request, Response> chain(
        AtomicBoolean proceeded
    ) {
        return (task, action, request, listener) -> proceeded.set(true);
    }

    private static final class TestCustom extends org.elasticsearch.cluster.AbstractNamedDiffable<Metadata.ProjectCustom>
        implements
            Metadata.ProjectCustom {

        static final String TYPE = "test_encrypted_custom_for_snapshot_warning";

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public org.elasticsearch.TransportVersion getMinimalSupportedVersion() {
            return org.elasticsearch.TransportVersion.current();
        }

        @Override
        public void writeTo(org.elasticsearch.common.io.stream.StreamOutput out) {}

        @Override
        public java.util.EnumSet<Metadata.XContentContext> context() {
            return java.util.EnumSet.of(Metadata.XContentContext.GATEWAY);
        }

        @Override
        public java.util.Iterator<? extends org.elasticsearch.xcontent.ToXContent> toXContentChunked(
            org.elasticsearch.xcontent.ToXContent.Params params
        ) {
            return java.util.Collections.emptyIterator();
        }
    }
}
