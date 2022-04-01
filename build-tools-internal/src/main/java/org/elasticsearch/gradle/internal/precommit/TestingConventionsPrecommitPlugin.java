/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.internal.InternalPlugin;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import java.util.stream.Collectors;

public class TestingConventionsPrecommitPlugin extends PrecommitPlugin implements InternalPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        TaskProvider<TestingConventionsTasks> testingConventions = project.getTasks()
            .register("testingConventions", TestingConventionsTasks.class, t -> {
                TestingConventionRule testsRule = t.getNaming().maybeCreate("Tests");
                testsRule.baseClass("org.apache.lucene.tests.util.LuceneTestCase");
                TestingConventionRule itRule = t.getNaming().maybeCreate("IT");
                itRule.baseClass("org.elasticsearch.test.ESIntegTestCase");
                itRule.baseClass("org.elasticsearch.test.rest.ESRestTestCase");
                t.setCandidateClassFilesProvider(
                    project.provider(
                        () -> project.getTasks()
                            .withType(Test.class)
                            .matching(Task::getEnabled)
                            .stream()
                            .collect(Collectors.toMap(Task::getPath, task -> task.getCandidateClassFiles().getFiles()))
                    )
                );
                SourceSetContainer javaSourceSets = GradleUtils.getJavaSourceSets(project);
                t.setSourceSets(javaSourceSets);
                // Run only after everything is compiled
                javaSourceSets.all(sourceSet -> t.dependsOn(sourceSet.getOutput().getClassesDirs()));
            });
        return testingConventions;
    }
}
