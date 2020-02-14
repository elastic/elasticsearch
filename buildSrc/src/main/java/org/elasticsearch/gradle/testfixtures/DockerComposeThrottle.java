package org.elasticsearch.gradle.testfixtures;

import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

public abstract class DockerComposeThrottle implements BuildService<BuildServiceParameters.None> {}
