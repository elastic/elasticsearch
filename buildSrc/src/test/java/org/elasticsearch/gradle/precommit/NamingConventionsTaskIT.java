package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.TaskOutcome;

import java.util.Arrays;

public class NamingConventionsTaskIT extends GradleIntegrationTestCase {

    public static final String SAMPLE_PROJECT = "namingConventionsSelfTest";

    public void testPluginCanBeApplied() {
        BuildResult result = getGradleRunner(SAMPLE_PROJECT)
            .withArguments("hello", "-s", "-PcheckForTestsInMain=false")
            .build();

        assertEquals(TaskOutcome.SUCCESS, result.task(":hello").getOutcome());
        String output = result.getOutput();
        assertTrue(output, output.contains("build plugin can be applied"));
    }

    public void testNameCheckFailsAsItShould() {
        BuildResult result = getGradleRunner(SAMPLE_PROJECT)
            .withArguments("namingConventions", "-s", "-PcheckForTestsInMain=false")
            .buildAndFail();

        assertNotNull("task did not run", result.task(":namingConventions"));
        assertEquals(TaskOutcome.FAILED, result.task(":namingConventions").getOutcome());
        String output = result.getOutput();
        for (String line : Arrays.asList(
            "Found inner classes that are tests, which are excluded from the test runner:",
            "* org.elasticsearch.test.NamingConventionsCheckInMainIT$InternalInvalidTests",
            "Classes ending with [Tests] must subclass [UnitTestCase]:",
            "* org.elasticsearch.test.NamingConventionsCheckInMainTests",
            "* org.elasticsearch.test.NamingConventionsCheckInMainIT",
            "Not all subclasses of UnitTestCase match the naming convention. Concrete classes must end with [Tests]:",
            "* org.elasticsearch.test.WrongName")) {
            assertTrue(
                "expected:  '" + line + "' but it was not found in the output:\n" + output,
                output.contains(line)
            );
        }
    }

    public void testNameCheckFailsAsItShouldWithMain() {
        BuildResult result = getGradleRunner(SAMPLE_PROJECT)
            .withArguments("namingConventions", "-s", "-PcheckForTestsInMain=true")
            .buildAndFail();

        assertNotNull("task did not run", result.task(":namingConventions"));
        assertEquals(TaskOutcome.FAILED, result.task(":namingConventions").getOutcome());

        String output = result.getOutput();
        for (String line : Arrays.asList(
            "Classes ending with [Tests] or [IT] or extending [UnitTestCase] must be in src/test/java:",
            "* org.elasticsearch.test.NamingConventionsCheckBadClasses$DummyInterfaceTests",
            "* org.elasticsearch.test.NamingConventionsCheckBadClasses$DummyAbstractTests",
            "* org.elasticsearch.test.NamingConventionsCheckBadClasses$InnerTests",
            "* org.elasticsearch.test.NamingConventionsCheckBadClasses$NotImplementingTests",
            "* org.elasticsearch.test.NamingConventionsCheckBadClasses$WrongNameTheSecond",
            "* org.elasticsearch.test.NamingConventionsCheckBadClasses$WrongName")) {
            assertTrue(
                "expected:  '" + line + "' but it was not found in the output:\n"+output,
                output.contains(line)
            );
        }
    }

}
