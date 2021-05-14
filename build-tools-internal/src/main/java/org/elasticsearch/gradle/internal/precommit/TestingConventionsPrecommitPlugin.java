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
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.TaskProvider;

public class TestingConventionsPrecommitPlugin extends PrecommitPlugin implements InternalPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        TaskProvider<TestingConventionsTasks> testingConventions = project.getTasks()
            .register("testingConventions", TestingConventionsTasks.class);
        testingConventions.configure(t -> {
            TestingConventionRule testsRule = t.getNaming().maybeCreate("Tests");
            testsRule.baseClass("org.apache.lucene.util.LuceneTestCase");
            TestingConventionRule itRule = t.getNaming().maybeCreate("IT");
            itRule.baseClass("org.elasticsearch.test.ESIntegTestCase");
            itRule.baseClass("org.elasticsearch.test.rest.ESRestTestCase");
        });
        return testingConventions;
    }
}
