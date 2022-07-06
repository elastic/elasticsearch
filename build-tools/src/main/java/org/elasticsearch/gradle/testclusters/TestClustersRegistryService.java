/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.testclusters;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.tooling.events.FinishEvent;
import org.gradle.tooling.events.OperationCompletionListener;
import org.gradle.tooling.events.task.TaskFailureResult;
import org.gradle.tooling.events.task.TaskFinishEvent;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public abstract class TestClustersRegistryService implements BuildService<BuildServiceParameters.None>, OperationCompletionListener {
     private final TestClustersRegistry registry;


    public TestClustersRegistryService() {
        registry = new TestClustersRegistry();
    }
    public void claimCluster(ElasticsearchCluster cluster) {
        registry.claimCluster(cluster);

    }

    public void maybeStartCluster(String taskPath, ElasticsearchCluster cluster) {
        registry.maybeStartCluster(taskPath, cluster);

    }

    @Override
    public void onFinish(FinishEvent finishEvent) {
        registry.onFinish(finishEvent);
    }

    public TestClustersRegistry getRegistry() {
        return registry;
    }
}
