/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ci;

import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

/**
 * Limits how many test tasks that start Elasticsearch clusters (REST, cluster and spec tests) a CI part job runs at
 * once, through the service's {@code maxParallelUsages}. Unlimited, a part started up to 16 of them on a 16-core
 * agent, each with a test JVM and one or more nodes; cluster start-up then took minutes instead of seconds and the
 * tests ran far longer than their usual durations. Plain unit test tasks are not limited.
 */
public abstract class CiHeavyTestThrottle implements BuildService<BuildServiceParameters.None> {

    /** Overrides how many heavy test tasks run at once; 0 turns the limit off. */
    public static final String MAX_PARALLEL_PROPERTY = "tests.ci.maxParallelHeavyTests";

    /** Whether test task {@code taskName} starts a cluster (or, for internal cluster tests, several nodes in-process). */
    public static boolean isHeavy(String taskName) {
        return taskName.contains("RestTest")
            || taskName.contains("RestCompatTest")
            || taskName.contains("ClusterTest")
            || taskName.startsWith("csvSpecTests")
            || taskName.equals("integTest")
            || taskName.equals("bcUpgradeTest");
    }

    /** The default limit: half the Gradle workers, the same share {@code TestClustersThrottle} gives cluster nodes. */
    public static int defaultLimit(int maxWorkers) {
        return Math.max(1, maxWorkers / 2);
    }
}
