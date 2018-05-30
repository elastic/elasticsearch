package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.GradleIntegrationTests;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.TaskOutcome;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class NamingConventionsTaskIT extends GradleIntegrationTests {

    @Test
    public void pluginCanBeApplied() {
        BuildResult result = GradleRunner.create()
            .withProjectDir(getProjectDir("namingConventionsSelfTest"))
            .withArguments("hello", "-s")
            .withPluginClasspath()
            .build();

        assertTrue(result.getOutput().contains("build plugin can be applied"));
        assertEquals(TaskOutcome.SUCCESS, result.task(":hello").getOutcome());
    }

}
