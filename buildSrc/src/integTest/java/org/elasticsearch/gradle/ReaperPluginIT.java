/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle;

import org.elasticsearch.gradle.internal.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.Before;

public class ReaperPluginIT extends GradleIntegrationTestCase {
    private GradleRunner runner;

    @Before
    public void setup() {
        runner = getGradleRunner("reaper");
    }

    public void testCanLaunchReaper() {
        BuildResult result = runner.withArguments(":launchReaper", "-S", "--info").build();
        assertTaskSuccessful(result, ":launchReaper");
        assertOutputContains(result.getOutput(), "Copying reaper.jar...");
    }
}
