package org.elasticsearch.gradle;

import org.elasticsearch.gradle.test.GradleIntegrationTestCase;
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
