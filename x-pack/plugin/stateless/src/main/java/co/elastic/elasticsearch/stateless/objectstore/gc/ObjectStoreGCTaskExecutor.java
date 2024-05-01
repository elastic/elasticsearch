/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.objectstore.gc;

import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class ObjectStoreGCTaskExecutor extends PersistentTasksExecutor<ObjectStoreGCTaskExecutor.ObjectStoreGCTaskParams>
    implements
        ClusterStateListener {
    private final Logger logger = LogManager.getLogger(ObjectStoreGCTaskExecutor.class);
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final PersistentTasksService persistentTasksService;
    private final StaleIndicesGCService staleIndicesGCService;
    private final StaleTranslogsGCService staleTranslogsGCService;

    private ObjectStoreGCTaskExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        Supplier<ObjectStoreService> objectStoreService,
        Settings settings
    ) {
        super(ObjectStoreGCTask.TASK_NAME, threadPool.generic());
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.staleIndicesGCService = new StaleIndicesGCService(objectStoreService, clusterService, threadPool, client);
        this.staleTranslogsGCService = new StaleTranslogsGCService(objectStoreService, clusterService, threadPool, client, settings);
        this.persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
    }

    public static ObjectStoreGCTaskExecutor create(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        Supplier<ObjectStoreService> objectStoreServiceSupplier,
        Settings settings
    ) {
        var executor = new ObjectStoreGCTaskExecutor(clusterService, threadPool, client, objectStoreServiceSupplier, settings);
        clusterService.addListener(executor);
        return executor;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        var clusterState = event.state();

        PersistentTasksCustomMetadata tasksCustomMetadata = clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (tasksCustomMetadata != null && tasksCustomMetadata.getTask(ObjectStoreGCTask.TASK_NAME) != null) {
            clusterService.removeListener(this);
            return;
        }

        if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)
            || clusterState.nodes().isLocalNodeElectedMaster() == false) {
            return;
        }

        persistentTasksService.sendStartRequest(
            ObjectStoreGCTask.TASK_NAME,
            ObjectStoreGCTask.TASK_NAME,
            new ObjectStoreGCTaskParams(),
            null,
            ActionListener.wrap(r -> logger.debug("Created the blob store gc task executor"), e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException == false) {
                    logger.error("Failed to create the blob store gc task", e);
                    assert false : "Unexpected failure while creating the object store persistent GC task";
                }
            })
        );
    }

    @Override
    protected AllocatedPersistentTask createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<ObjectStoreGCTaskParams> taskInProgress,
        Map<String, String> headers
    ) {
        return new ObjectStoreGCTask(
            id,
            type,
            action,
            ObjectStoreGCTask.TASK_NAME,
            parentTaskId,
            headers,
            staleIndicesGCService,
            staleTranslogsGCService,
            threadPool,
            clusterService.getSettings()
        );
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, ObjectStoreGCTaskParams params, PersistentTaskState state) {
        ((ObjectStoreGCTask) task).runGC();
    }

    @Override
    protected DiscoveryNode selectLeastLoadedNode(
        ClusterState clusterState,
        Collection<DiscoveryNode> candidateNodes,
        Predicate<DiscoveryNode> selector
    ) {
        return super.selectLeastLoadedNode(
            clusterState,
            candidateNodes,
            selector.and(n -> n.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()))
        );
    }

    public static class ObjectStoreGCTaskParams implements PersistentTaskParams {
        public static final ObjectStoreGCTaskParams INSTANCE = new ObjectStoreGCTaskParams();
        public static final ObjectParser<ObjectStoreGCTaskParams, Void> PARSER = new ObjectParser<>(
            ObjectStoreGCTask.TASK_NAME,
            false,
            () -> INSTANCE
        );

        ObjectStoreGCTaskParams() {}

        public ObjectStoreGCTaskParams(StreamInput ignored) {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return ObjectStoreGCTask.TASK_NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_12_0;
        }

        @Override
        public void writeTo(StreamOutput out) {}

        public static ObjectStoreGCTaskParams fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ObjectStoreGCTaskParams;
        }
    }
}
