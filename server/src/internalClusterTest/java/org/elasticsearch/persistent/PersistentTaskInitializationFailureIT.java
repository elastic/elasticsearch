/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.persistent;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;

public class PersistentTaskInitializationFailureIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(FailingInitializationPersistentTasksPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        // We need to pass the plugin to the transport client in order to get FailingInitializationTaskParams registered
        // into the transport client NamedWriteableRegistry
        return Collections.singletonList(FailingInitializationPersistentTasksPlugin.class);
    }

    public void testPersistentTasksThatFailDuringInitializationAreRemovedFromClusterState() throws Exception {
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        PlainActionFuture<PersistentTasksCustomMetadata.PersistentTask<FailingInitializationTaskParams>> startPersistentTaskFuture =
            new PlainActionFuture<>();
        persistentTasksService.sendStartRequest(
            UUIDs.base64UUID(),
            FailingInitializationPersistentTaskExecutor.TASK_NAME,
            new FailingInitializationTaskParams(),
            startPersistentTaskFuture
        );
        startPersistentTaskFuture.actionGet();

        assertBusy(() -> {
            Metadata metadata = client().admin().cluster().prepareState().execute().actionGet().getState().getMetadata();
            final PersistentTasksCustomMetadata persistentTasks = metadata.custom(PersistentTasksCustomMetadata.TYPE);
            assertThat(persistentTasks.tasks().toString(), persistentTasks.tasks(), empty());
        });
    }

    public static class FailingInitializationPersistentTasksPlugin extends Plugin implements PersistentTaskPlugin {
        public FailingInitializationPersistentTasksPlugin(Settings settings) {}

        @Override
        public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
            ClusterService clusterService,
            ThreadPool threadPool,
            Client client,
            SettingsModule settingsModule,
            IndexNameExpressionResolver expressionResolver
        ) {
            return Collections.singletonList(new FailingInitializationPersistentTaskExecutor());
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return Collections.singletonList(
                new NamedWriteableRegistry.Entry(
                    PersistentTaskParams.class,
                    FailingInitializationPersistentTaskExecutor.TASK_NAME,
                    FailingInitializationTaskParams::new
                )
            );
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return Collections.singletonList(
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    new ParseField(FailingInitializationPersistentTaskExecutor.TASK_NAME),
                    p -> {
                        p.skipChildren();
                        return new FailingInitializationTaskParams();
                    }
                )
            );
        }
    }

    public static class FailingInitializationTaskParams implements PersistentTaskParams {
        public FailingInitializationTaskParams() {}

        public FailingInitializationTaskParams(StreamInput in) throws IOException {}

        @Override
        public String getWriteableName() {
            return FailingInitializationPersistentTaskExecutor.TASK_NAME;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }
    }

    static class FailingInitializationPersistentTaskExecutor extends PersistentTasksExecutor<FailingInitializationTaskParams> {
        static final String TASK_NAME = "cluster:admin/persistent/test_init_failure";
        static final String EXECUTOR_NAME = "failing_executor";

        FailingInitializationPersistentTaskExecutor() {
            super(TASK_NAME, EXECUTOR_NAME);
        }

        @Override
        protected AllocatedPersistentTask createTask(
            long id,
            String type,
            String action,
            TaskId parentTaskId,
            PersistentTasksCustomMetadata.PersistentTask<FailingInitializationTaskParams> taskInProgress,
            Map<String, String> headers
        ) {
            return new AllocatedPersistentTask(id, type, action, "", parentTaskId, headers) {
                @Override
                protected void init(
                    PersistentTasksService persistentTasksService,
                    TaskManager taskManager,
                    String persistentTaskId,
                    long allocationId
                ) {
                    throw new RuntimeException("BOOM");
                }
            };
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, FailingInitializationTaskParams params, PersistentTaskState state) {
            assert false : "Unexpected call";
        }
    }
}
