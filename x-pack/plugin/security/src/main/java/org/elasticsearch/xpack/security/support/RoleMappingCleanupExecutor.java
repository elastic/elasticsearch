/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;
import org.elasticsearch.xpack.core.security.support.RoleMappingCleanupTaskParams;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class RoleMappingCleanupExecutor extends PersistentTasksExecutor<RoleMappingCleanupTaskParams>
    implements
        BiConsumer<SecurityIndexManager.State, SecurityIndexManager.State> {

    private static final Logger logger = LogManager.getLogger(RoleMappingCleanupExecutor.class);
    private final NativeRoleMappingStore roleMappingStore;

    private final ClusterService clusterService;
    private AllocatedPersistentTask currentTask = null;
    private final SecurityIndexManager securityIndexManager;
    private final AtomicBoolean cleanupInProgress = new AtomicBoolean(false);

    public RoleMappingCleanupExecutor(
        ClusterService clusterService,
        String taskName,
        Executor executor,
        NativeRoleMappingStore roleMappingStore,
        SecurityIndexManager securityIndexManager
    ) {
        super(taskName, executor);
        this.clusterService = clusterService;
        this.roleMappingStore = roleMappingStore;
        this.securityIndexManager = securityIndexManager;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, RoleMappingCleanupTaskParams params, PersistentTaskState state) {
        currentTask = task;
        if (securityIndexManager.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS) == false) {
            logger.info("Security index not available, waiting...");
            securityIndexManager.addStateListener(this);
        } else {
            doCleanup();
        }
    }

    private void doCleanup() {
        getRoleMappingBothInClusterStateAndIndex(
            clusterService.state(),
            ActionListener.wrap(roleMappings -> disableRoleMappings(roleMappings, ActionListener.wrap(response -> {
                markAsCompleted();
            }, this::markAsFailed)), this::markAsFailed)
        );
    }

    private void markAsCompleted() {
        currentTask.markAsCompleted();
        logger.info("Successfully cleaned up role mappings");
        cleanupInProgress.set(false);
    }

    private void markAsFailed(Exception exception) {
        currentTask.markAsFailed(exception);
        logger.warn("Role mapping clean up failed: " + exception);
        cleanupInProgress.set(false);
    }

    private void getRoleMappingBothInClusterStateAndIndex(ClusterState state, ActionListener<List<ExpressionRoleMapping>> listener) {
        RoleMappingMetadata roleMappingMetadata = RoleMappingMetadata.getFromClusterState(state);
        Set<String> roleMappingNames = roleMappingMetadata.getRoleMappings()
            .stream()
            .map(ExpressionRoleMapping::getName)
            .collect(Collectors.toSet());

        if (roleMappingNames.isEmpty()) {
            listener.onResponse(List.of());
            return;
        }
        roleMappingStore.getRoleMappings(roleMappingNames, listener);
    }

    private void disableRoleMappings(List<ExpressionRoleMapping> roleMappings, ActionListener<Collection<Void>> listener) {
        GroupedActionListener<Void> groupedActionListener = new GroupedActionListener<>(roleMappings.size(), listener);
        for (ExpressionRoleMapping roleMapping : roleMappings) {
            var request = new PutRoleMappingRequest();
            request.setName(roleMapping.getName());
            request.setEnabled(false);
            request.setRoles(roleMapping.getRoles());
            request.setRoleTemplates(roleMapping.getRoleTemplates());
            request.setRules(roleMapping.getExpression());
            request.setMetadata(roleMapping.getMetadata());
            roleMappingStore.putRoleMapping(
                request,
                ActionListener.wrap(response -> groupedActionListener.onResponse(null), groupedActionListener::onFailure)
            );
        }
    }

    @Override
    public void accept(SecurityIndexManager.State oldState, SecurityIndexManager.State newState) {
        if (cleanupInProgress.compareAndSet(false, true)
            && securityIndexManager.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)) {
            securityIndexManager.removeStateListener(this);
            doCleanup();
        }
    }
}
