/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rerun;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.testing.Test;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.test.rerun.TestTaskConfigurer.configureTestTask;

public class TestRerunPlugin implements Plugin<Project> {

    private final ObjectFactory objectFactory;

    @Inject
    TestRerunPlugin(ObjectFactory objectFactory) {
        this.objectFactory = objectFactory;
    }

    @Override
    public void apply(Project project) {
        project.getTasks().withType(Test.class).configureEach(task -> configureTestTask(task, objectFactory));
    }

}
