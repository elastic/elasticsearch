/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.internal.ExportElasticsearchBuildResourcesTask;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.internal.precommit.InternalPrecommitTasks;
import org.elasticsearch.gradle.internal.test.rest.LegacyJavaRestTestPlugin;
import org.elasticsearch.gradle.internal.test.rest.LegacyYamlRestTestPlugin;
import org.elasticsearch.gradle.internal.test.rest.RestTestUtil;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.testing.Test;
import org.gradle.plugins.ide.eclipse.model.EclipseModel;
import org.gradle.plugins.ide.idea.model.IdeaModel;

import java.util.Arrays;
import java.util.Map;

/**
 * Configures the build to compile tests against Elasticsearch's test framework
 * and run REST tests. Use BuildPlugin if you want to build main code as well
 * as tests.
 *
 * @deprecated use {@link InternalClusterTestPlugin}, {@link LegacyJavaRestTestPlugin} or
 * {@link LegacyYamlRestTestPlugin} instead.
 */
@Deprecated
public class StandaloneRestTestPlugin implements Plugin<Project> {
    @Override
    public void apply(final Project project) {
        if (project.getPluginManager().hasPlugin("elasticsearch.build")) {
            throw new InvalidUserDataException(
                "elasticsearch.standalone-test, elasticsearch.standalone-rest-test, " + "and elasticsearch.build are mutually exclusive"
            );
        }

        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        project.getPluginManager().apply(LegacyRestTestBasePlugin.class);

        project.getTasks().register("buildResources", ExportElasticsearchBuildResourcesTask.class);

        // only setup tests to build
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        final SourceSet testSourceSet = sourceSets.create("test");

        project.getTasks().withType(Test.class).configureEach(test -> {
            test.setTestClassesDirs(testSourceSet.getOutput().getClassesDirs());
            test.setClasspath(testSourceSet.getRuntimeClasspath());
        });

        // create a compileOnly configuration as others might expect it
        project.getConfigurations().create("compileOnly");
        RestTestUtil.setupJavaRestTestDependenciesDefaults(project, testSourceSet);

        EclipseModel eclipse = project.getExtensions().getByType(EclipseModel.class);
        eclipse.getClasspath().setSourceSets(Arrays.asList(testSourceSet));
        eclipse.getClasspath()
            .setPlusConfigurations(
                Arrays.asList(project.getConfigurations().getByName(JavaPlugin.TEST_RUNTIME_CLASSPATH_CONFIGURATION_NAME))
            );

        IdeaModel idea = project.getExtensions().getByType(IdeaModel.class);
        idea.getModule().getTestSources().from(testSourceSet.getJava().getSrcDirs());
        idea.getModule()
            .getScopes()
            .put(
                "TEST",
                Map.of("plus", Arrays.asList(project.getConfigurations().getByName(JavaPlugin.TEST_RUNTIME_CLASSPATH_CONFIGURATION_NAME)))
            );
        InternalPrecommitTasks.create(project, false);
    }
}
