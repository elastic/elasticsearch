/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.testclusters;

import org.elasticsearch.gradle.FileSystemOperationsAware;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.internal.BuildServiceRegistryInternal;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.WorkResult;
import org.gradle.api.tasks.options.Option;
import org.gradle.api.tasks.testing.Test;
import org.gradle.internal.resources.ResourceLock;
import org.gradle.internal.resources.SharedResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.gradle.testclusters.TestClustersPlugin.THROTTLE_SERVICE_NAME;

/**
 * Customized version of Gradle {@link Test} task which tracks a collection of {@link ElasticsearchCluster} as a task input. We must do this
 * as a custom task type because the current {@link org.gradle.api.tasks.TaskInputs} runtime API does not have a way to register
 * {@link Nested} inputs.
 */
@CacheableTask
public class StandaloneRestIntegTestTask extends Test implements TestClustersAware, FileSystemOperationsAware {

    private Collection<ElasticsearchCluster> clusters = new HashSet<>();
    private boolean debugServer = false;

    public StandaloneRestIntegTestTask() {
        this.getOutputs()
            .doNotCacheIf(
                "Caching disabled for this task since it uses a cluster shared by other tasks",
                /*
                 * Look for any other tasks which use the same cluster as this task. Since tests often have side effects for the cluster
                 * they execute against, this state can cause issues when trying to cache tests results of tasks that share a cluster. To
                 * avoid any undesired behavior we simply disable the cache if we detect that this task uses a cluster shared between
                 * multiple tasks.
                 */
                t -> getProject().getTasks()
                    .withType(StandaloneRestIntegTestTask.class)
                    .stream()
                    .filter(task -> task != this)
                    .anyMatch(task -> Collections.disjoint(task.getClusters(), getClusters()) == false)
            );

        this.getOutputs()
            .doNotCacheIf(
                "Caching disabled for this task since it is configured to preserve data directory",
                // Don't cache the output of this task if it's not running from a clean data directory.
                t -> getClusters().stream().anyMatch(cluster -> cluster.isPreserveDataDir())
            );
    }

    @Option(option = "debug-server-jvm", description = "Enable debugging configuration, to allow attaching a debugger to elasticsearch.")
    public void setDebugServer(boolean enabled) {
        this.debugServer = enabled;
        systemProperty("tests.cluster.debug.enabled", Boolean.toString(enabled));
    }

    @Nested
    @Override
    public Collection<ElasticsearchCluster> getClusters() {
        return clusters;
    }

    @Override
    @Internal
    public List<ResourceLock> getSharedResources() {
        List<ResourceLock> locks = new ArrayList<>(super.getSharedResources());
        BuildServiceRegistryInternal serviceRegistry = getServices().get(BuildServiceRegistryInternal.class);
        Provider<TestClustersThrottle> throttleProvider = GradleUtils.getBuildService(serviceRegistry, THROTTLE_SERVICE_NAME);
        SharedResource resource = serviceRegistry.forService(throttleProvider);

        int nodeCount = clusters.stream().mapToInt(cluster -> cluster.getNodes().size()).sum();
        if (nodeCount > 0) {
            for (int i = 0; i < Math.min(nodeCount, resource.getMaxUsages()); i++) {
                locks.add(resource.getResourceLock());
            }
        }
        return Collections.unmodifiableList(locks);
    }

    public WorkResult delete(Object... objects) {
        return getFileSystemOperations().delete(d -> d.delete(objects));
    }

    @Override
    public void beforeStart() {
        if (debugServer) {
            enableDebug();
        }
    }
}
