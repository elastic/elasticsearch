/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.IngestService.BulkPipelineCreateOperation;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.metadata.BulkMetadataService.BulkMetadataOperation;
import static org.elasticsearch.cluster.metadata.BulkMetadataService.BulkMetadataOperationContext;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.BulkTemplateOperation;

public class MetadataContentService {

    private static final Logger logger = LogManager.getLogger(MetadataContentService.class);

    private final ClusterService clusterService;
    private final MasterServiceTaskQueue<ContentPackClusterStateUpdateTask> taskQueue;

    // PRTODO: These should be injected as a registry already
    private final MetadataIndexTemplateService metadataIndexTemplateService;
    private final IngestService ingestService;

    /**
     * This is the cluster state task executor for all bulk content installation actions.
     */
    private static final SimpleBatchedExecutor<ContentPackClusterStateUpdateTask, Void> CONTENT_TASK_EXECUTOR =
        new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Void> executeTask(ContentPackClusterStateUpdateTask task, ClusterState clusterState)
                throws Exception {
                return Tuple.tuple(task.execute(clusterState), null);
            }

            @Override
            public void taskSucceeded(ContentPackClusterStateUpdateTask task, Void unused) {
                task.listener.onResponse(AcknowledgedResponse.TRUE);
            }
        };

    /**
     * A specialized cluster state update task that always takes a listener handling an
     * AcknowledgedResponse, as all template actions have simple acknowledged yes/no responses.
     */
    private abstract static class ContentPackClusterStateUpdateTask implements ClusterStateTaskListener {
        final ActionListener<AcknowledgedResponse> listener;

        ContentPackClusterStateUpdateTask(
            ActionListener<AcknowledgedResponse> listener
        ) {
            this.listener = listener;
        }

        public abstract ClusterState execute(ClusterState currentState) throws Exception;

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    // PRTODO: This needs to be constructed at node creation time from plugins
    final Map<String, ServiceOperationFactory> serviceRegistry = new HashMap<>();

    @Inject
    public MetadataContentService(
        ClusterService clusterService,
        MetadataIndexTemplateService metadataIndexTemplateService,
        IngestService ingestService
    ) {
        this.clusterService = clusterService;
        this.taskQueue = clusterService.createTaskQueue("plugin-contents", Priority.URGENT, CONTENT_TASK_EXECUTOR);
        this.metadataIndexTemplateService = metadataIndexTemplateService;
        this.ingestService = ingestService;

        serviceRegistry.put(
            metadataIndexTemplateService.getBulkMetadataServiceName(),
            (BulkMetadataOperation op) -> new ServiceOperation<>(metadataIndexTemplateService, (BulkTemplateOperation) op)
        );

        serviceRegistry.put(
            ingestService.getBulkMetadataServiceName(),
            (BulkMetadataOperation op) -> new ServiceOperation<>(ingestService, (BulkPipelineCreateOperation) op)
        );
    }

    public record IngestPipelineOperation(String id, BytesReference content, XContentType xContentType) {}

    /**
     * The collection of contents contained in this content pack that will be bulk updated/installed
     */
    public static final class ContentPack {
        private final Map<String, BulkMetadataOperation> packageContents;
        private final TimeValue masterTimeout;

        private ContentPack(
            Map<String, BulkMetadataOperation> packageContents,
            TimeValue masterTimeout
        ) {
            this.packageContents = packageContents;
            this.masterTimeout = masterTimeout;
        }

        public Collection<BulkMetadataOperation> packageContents() {
            return packageContents.values();
        }

        public TimeValue masterTimeout() {
            return masterTimeout;
        }

        public static Builder builder(TimeValue masterTimeout) {
            return new Builder(masterTimeout);
        }

        public static class Builder {
            private final Map<String, BulkMetadataOperation> packageContents = new HashMap<>();
            private TimeValue masterTimeout;

            private Builder(TimeValue masterTimeout) {
                this.masterTimeout = Objects.requireNonNull(masterTimeout, "masterTimeout");
            }

            public Builder addToPackage(BulkMetadataOperation operation) {
                BulkMetadataOperation existingOperation = packageContents.get(operation.getBulkMetadataServiceName());
                if (existingOperation != null) {
                    throw new IllegalArgumentException(
                        "Trying to add operation of type ["
                            + operation.getBulkMetadataServiceName()
                            + "] but one is already present in this package"
                    );
                }
                packageContents.put(operation.getBulkMetadataServiceName(), operation);
                return this;
            }

            public Builder masterTimeout(TimeValue masterTimeout) {
                this.masterTimeout = Objects.requireNonNull(masterTimeout, "masterTimeout");
                return this;
            }

            public ContentPack build() {
                return new ContentPack(Collections.unmodifiableMap(packageContents), masterTimeout);
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (ContentPack) obj;
            return Objects.equals(this.packageContents, that.packageContents) &&
                Objects.equals(this.masterTimeout, that.masterTimeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(packageContents, masterTimeout);
        }

        @Override
        public String toString() {
            return "ContentPack[" +
                "packageContents=" + packageContents + ", " +
                "masterTimeout=" + masterTimeout + ']';
        }
    }

    /**
     * A binding of a service to a specific bulk operation, dependency, and context.
     */
    private static class ServiceOperation<B extends BulkMetadataOperation, D, C> {
        private final BulkMetadataService<B, D, C> service;
        private B operations;
        private D dependency;
        private C context;

        private ServiceOperation(BulkMetadataService<B, D, C> service, B operations) {
            this.service = service;
            this.operations = operations;
            this.dependency = null;
            this.context = null;
        }

        void validateBatch(ClusterState currentState) throws Exception {
            if (operations.isEmpty() == false) {
                service.validateBatch(operations, currentState);
            }
        }

        void filterBatch(ClusterState currentState) {
            if (operations.isEmpty() == false) {
                operations = service.filterBatch(operations, currentState);
            }
        }

        void collectDependencies(Client client, ActionListener<Void> listener) {
            if (operations.isEmpty()) {
                listener.onResponse(null);
            } else {
                service.collectDependencies(client, listener.delegateFailureAndWrap((l, dep) -> {
                    dependency = dep;
                    l.onResponse(null);
                }));
            }
        }

        void dependencyValidation(ClusterState currentState) throws Exception {
            if (operations.isEmpty() == false) {
                service.dependencyValidation(operations, currentState, dependency);
            }
        }

        boolean applyBatch(ClusterState currentState, Metadata.Builder metadataBuilder) throws Exception {
            if (operations.isEmpty() == false) {
                BulkMetadataOperationContext<C> ctx = service.applyBatch(operations, currentState, metadataBuilder);
                if (ctx == null) {
                    return false;
                } else {
                    context = ctx.getContext();
                    return ctx.isMetadataModified();
                }
            } else {
                return false;
            }
        }

        void validateFinalClusterState(ClusterState previousState, ClusterState newState) throws Exception {
            if (operations.isEmpty() == false) {
                service.validateFinalClusterState(previousState, newState, context);
            }
        }
    }

    // PRTODO: This should be the interface provided by plugins that serve up these operations
    private interface ServiceOperationFactory {
        ServiceOperation<?, ?, ?> create(BulkMetadataOperation operation);
    }

    private static final class OrCollector implements Consumer<Boolean> {
        private boolean value = false;

        @Override
        public void accept(Boolean aBoolean) {
            value |= aBoolean;
        }

        public boolean result() {
            return value;
        }
    }

    public void installContentPack(
        final String cause,
        final ContentPack contentPack,
        final Client client,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        ConcurrentHashMap<String, ServiceOperation<?, ?, ?>> services = new ConcurrentHashMap<>();
        for (Map.Entry<String, BulkMetadataOperation> packageEntry : contentPack.packageContents.entrySet()) {
            ServiceOperationFactory serviceOperationFactory = serviceRegistry.get(packageEntry.getKey());
            if (serviceOperationFactory == null) {
                throw new IllegalArgumentException("Invalid package type [" + packageEntry.getKey() + "] given, no factory registered");
            }
            BulkMetadataOperation batchOperation = packageEntry.getValue();
            ServiceOperation<?, ?, ?> serviceOperation = serviceOperationFactory.create(batchOperation);
            services.put(packageEntry.getKey(), serviceOperation);
        }

        SubscribableListener.newForked((ActionListener<Collection<Void>> l) -> {
            // PRTODO: Is this cluster state by-definition less fresh than the one we maintain in the IngestService?
            //  Is this safe to check against?
            ClusterState clusterState = clusterService.state();
            forEachService(services, (service) -> service.validateBatch(clusterState));
            forEachService(services, (service) -> service.filterBatch(clusterState));

            // PRTODO: Make sure that this correctly returns immediately if not applicable
            GroupedActionListener<Void> dependencies = new GroupedActionListener<>(services.size(), l);
            forEachService(services, (service) -> service.collectDependencies(client, dependencies));
        }).andThen((ActionListener<AcknowledgedResponse> l, Collection<Void> depResults) -> {
            ClusterState clusterState = clusterService.state();
            forEachService(services, (service) -> service.dependencyValidation(clusterState));
            taskQueue.submitTask(
                "create-content-pack, cause [" + cause + "]",
                new ContentPackClusterStateUpdateTask(l) {
                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                        OrCollector stateModified = new OrCollector();
                        // PRTODO: The problem here is that each service takes its current view of the state from the currentState
                        //  but there's no way to guarantee that the "currentView" of the content hasn't already been invalidated
                        forEachServiceAccumulate(services, (service) -> service.applyBatch(currentState, metadataBuilder), stateModified);
                        if (stateModified.result()) {
                            ClusterState candidateState = ClusterState.builder(currentState).metadata(metadataBuilder).build();
                            forEachService(services, (service) -> service.validateFinalClusterState(currentState, candidateState));
                            return candidateState;
                        } else {
                            return currentState;
                        }
                    }
                },
                contentPack.masterTimeout
            );
        }).addListener(listener);
    }

    private static void forEachService(
        ConcurrentHashMap<String, ServiceOperation<?, ?, ?>> services,
        CheckedConsumer<ServiceOperation<?, ?, ?>, Exception> operation
    ) throws Exception {
        for (ServiceOperation<?, ?, ?> serviceOperation : services.values()) {
            operation.accept(serviceOperation);
        }
    }

    private static <T> void forEachServiceAccumulate(
        ConcurrentHashMap<String, ServiceOperation<?, ?, ?>> services,
        CheckedFunction<ServiceOperation<?, ?, ?>, T, Exception> operation,
        final Consumer<T> accumulator
    ) throws Exception {
        forEachService(services, (service) -> {
            T result = operation.apply(service);
            accumulator.accept(result);
        });
    }
}
