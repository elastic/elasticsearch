/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.conventions;

import org.elasticsearch.gradle.internal.conventions.info.ParallelDetector;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.testing.Test;

public class BasicConventionsPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        int defaultParallel = ParallelDetector.findDefaultParallel(project);
        project.getTasks().withType(Test.class).configureEach(test -> test.setMaxParallelForks(defaultParallel));
    }

}
