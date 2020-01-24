package org.elasticsearch.gradle.testclusters;

import org.elasticsearch.gradle.tool.Boilerplate;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.internal.BuildServiceRegistryInternal;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Nested;
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
public class RestTestRunnerTask extends Test implements TestClustersAware {

    private Collection<ElasticsearchCluster> clusters = new HashSet<>();

    public RestTestRunnerTask() {
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
                    .withType(RestTestRunnerTask.class)
                    .stream()
                    .filter(task -> task != this)
                    .anyMatch(task -> Collections.disjoint(task.getClusters(), getClusters()) == false)
            );
    }

    @Override
    public int getMaxParallelForks() {
        return 1;
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
        Provider<TestClustersThrottle> throttleProvider = Boilerplate.getBuildService(serviceRegistry, THROTTLE_SERVICE_NAME);
        SharedResource resource = serviceRegistry.forService(throttleProvider);

        int nodeCount = clusters.stream().mapToInt(cluster -> cluster.getNodes().size()).sum();
        if (nodeCount > 0) {
            locks.add(resource.getResourceLock(Math.min(nodeCount, resource.getMaxUsages())));
        }

        return Collections.unmodifiableList(locks);
    }
}
