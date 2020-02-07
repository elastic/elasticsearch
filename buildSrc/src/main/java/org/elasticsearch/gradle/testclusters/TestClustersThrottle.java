package org.elasticsearch.gradle.testclusters;

import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

public abstract class TestClustersThrottle implements BuildService<BuildServiceParameters.None> {}
