package org.elasticsearch.gradle.internal.test.rest;

import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.internal.tasks.TaskDependencyFactory;
import org.gradle.api.plugins.jvm.internal.DefaultJvmTestSuite;
import org.gradle.api.tasks.SourceSetContainer;

public abstract class DefaultYamlRestTestSuite extends DefaultJvmTestSuite implements YamlRestTestSuite {
    public DefaultYamlRestTestSuite(
        String name,
        SourceSetContainer sourceSets,
        ConfigurationContainer configurations,
        TaskDependencyFactory taskDependencyFactory
    ) {
        super(name, sourceSets, configurations, taskDependencyFactory);
    }
}
