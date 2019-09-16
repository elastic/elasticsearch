package org.elasticsearch.gradle.testclusters;

import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.testing.Test;

import java.util.Collection;
import java.util.HashSet;

import static org.elasticsearch.gradle.testclusters.TestDistribution.INTEG_TEST;

/**
 * Customized version of Gradle {@link Test} task which tracks a collection of {@link ElasticsearchCluster} as a task input. We must do this
 * as a custom task type because the current {@link org.gradle.api.tasks.TaskInputs} runtime API does not have a way to register
 * {@link Nested} inputs.
 */
@CacheableTask
public class RestTestRunnerTask extends Test implements TestClustersAware {

    private Collection<ElasticsearchCluster> clusters = new HashSet<>();

    public RestTestRunnerTask() {
        super();
        this.getOutputs().doNotCacheIf("Build cache is only enabled for tests against clusters using the 'integ-test' distribution",
            task -> clusters.stream().flatMap(c -> c.getNodes().stream()).anyMatch(n -> n.getTestDistribution() != INTEG_TEST));
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

}
