package org.elasticsearch.gradle.test;

import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.testing.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.elasticsearch.gradle.Distribution.INTEG_TEST;

/**
 * Customized version of Gradle {@link Test} task which tracks a collection of {@link ElasticsearchCluster} as a task input. We must do this
 * as a custom task type because the current {@link org.gradle.api.tasks.TaskInputs} runtime API does not have a way to register
 * {@link Nested} inputs.
 */
@CacheableTask
public class RestTestRunnerTask extends Test {

    private Collection<ElasticsearchCluster> clusters = new ArrayList<>();

    public RestTestRunnerTask() {
        super();
        this.getOutputs().doNotCacheIf("Build cache is only enabled for tests against clusters using the 'integ-test' distribution",
            task -> clusters.stream().flatMap(c -> c.getNodes().stream()).anyMatch(n -> n.getDistribution() != INTEG_TEST));
    }

    @Nested
    @Optional
    public Collection<ElasticsearchCluster> getClusters() {
        return clusters;
    }

    public void testCluster(ElasticsearchCluster cluster) {
        this.clusters.add(cluster);
    }
}
