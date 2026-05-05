/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.OS;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.testing.Test;

public class SkipTestsOnWindowsPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        project.getTasks()
            .withType(Test.class)
            .configureEach(test -> test.onlyIf("Not running on windows", task -> OS.current() != OS.WINDOWS));
    }
}
