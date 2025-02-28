/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;

import static org.elasticsearch.cluster.metadata.BulkMetadataService.*;

/**
 * A metadata service that allows for bulk modifications to the cluster state. Most cluster state changes are batched in some form, but
 * the batched operation simply executes separate changes to the cluster state in a serial manner. Services that implement this interface
 * are able to group multiple operations together to be applied to a cluster state change atomically, as a single operation in a batched
 * cluster state update.
 */
public interface BulkMetadataService<B extends BulkMetadataOperation, D, C> {

    abstract class BulkMetadataOperation {
        protected String serviceKey;

        public BulkMetadataOperation(String serviceKey) {
            this.serviceKey = serviceKey;
        }

        public String getBulkMetadataServiceName() {
            return serviceKey;
        }

        public abstract boolean isEmpty();
    }

    class BulkMetadataOperationContext<T> {
        public static <T> BulkMetadataOperationContext<T> stateModified(T context) {
            return new BulkMetadataOperationContext<>(true, context);
        }

        public static BulkMetadataOperationContext<Void> stateModified() {
            return new BulkMetadataOperationContext<>(true, null);
        }

        public static <T> BulkMetadataOperationContext<T> stateUnmodified(T context) {
            return new BulkMetadataOperationContext<>(false, context);
        }

        public static BulkMetadataOperationContext<Void> stateUnmodified() {
            return new BulkMetadataOperationContext<>(false, null);
        }

        private final boolean metadataModified;
        private final T context;

        private BulkMetadataOperationContext(boolean metadataModified, T context) {
            this.metadataModified = metadataModified;
            this.context = context;
        }

        public boolean isMetadataModified() {
            return metadataModified;
        }

        public T getContext() {
            return context;
        }
    }

    String getBulkMetadataServiceName();

    /**
     * Executes to determine if this batch of operations is valid before doing any further work. Identifies any immediate validation issues
     * before submitting a cluster state update task.
     * @param batch operations to perform in bulk
     * @param currentState state of the cluster
     */
    default void validateBatch(B batch, ClusterState currentState) throws Exception {}

    /**
     * Removes or adds entries to the operation list before doing any further work. Commonly used to remove entries from the batch that
     * would be no-ops on the current cluster state.
     * @param batch operations to perform in bulk
     * @param currentState state of the cluster
     * @return the filtered list of operations to perform
     */
    default B filterBatch(B batch, ClusterState currentState) {
        return batch;
    }

    /**
     * Collects and constructs dependencies for this bulk operation. Some operations may need to collect information from other nodes
     * before applying the change to the cluster state. This is executed before the state change is submitted.
     * @param client to collect the dependency from
     * @param listener to collect the dependency through
     */
    default void collectDependencies(Client client, ActionListener<D> listener) {
        listener.onResponse(null);
    }

    /**
     * Validates the batch of operations against any dependencies collected by this service. Commonly used to check if the operations are
     * supported by the cluster.
     * @param batch operations to perform in bulk
     * @param currentState state of the cluster
     * @param dependency object collected by the service previously to validate against
     */
    default void dependencyValidation(B batch, ClusterState currentState, D dependency) throws Exception {}

    /**
     * Applies the batch of operations to the metadata builder, optionally returning a context object to use when validating the final
     * cluster state.
     * @param batch operations to perform in bulk
     * @param previousState state of the cluster
     * @param accumulator metadata builder to accumulate changes in
     * @return a context object that is used to carry forward state in order to validate the final cluster state
     */
    BulkMetadataOperationContext<C> applyBatch(B batch, ClusterState previousState, Metadata.Builder accumulator) throws Exception;

    /**
     * Uses the context object to validate the new cluster state before returning it.
     * @param previousState previous cluster state
     * @param newState candidate cluster state
     * @param context context for the applied cluster state operation
     */
    default void validateFinalClusterState(ClusterState previousState, ClusterState newState, C context) throws Exception {}
}
