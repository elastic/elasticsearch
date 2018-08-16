package org.elasticsearch.gradle.test;

import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.BuildTask;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.TaskOutcome;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class GradleIntegrationTestCase extends GradleUnitTestCase {

    protected File getProjectDir(String name) {
        File root = new File("src/testKit/");
        if (root.exists() == false) {
            throw new RuntimeException("Could not find resources dir for integration tests. " +
                "Note that these tests can only be ran by Gradle and are not currently supported by the IDE");
        }
        return new File(root, name).getAbsoluteFile();
    }

    protected GradleRunner getGradleRunner(String sampleProject) {
        return GradleRunner.create()
            .withProjectDir(getProjectDir(sampleProject))
            .withPluginClasspath();
    }

    protected File getBuildDir(String name) {
        return new File(getProjectDir(name), "build");
    }

    protected void assertOutputContains(String output, String... lines) {
        for (String line : lines) {
            assertOutputContains(output, line);
        }
        List<Integer> index = Stream.of(lines).map(line -> output.indexOf(line)).collect(Collectors.toList());
        if (index.equals(index.stream().sorted().collect(Collectors.toList())) == false) {
            fail("Expected the following lines to appear in this order:\n" +
                Stream.of(lines).map(line -> "   - `" + line + "`").collect(Collectors.joining("\n")) +
                "\nBut they did not. Output is:\n\n```" + output + "\n```\n"
            );
        }
    }

    protected void assertOutputContains(String output, String line) {
        assertTrue(
            "Expected the following line in output:\n\n" + line + "\n\nOutput is:\n" + output,
            output.contains(line)
        );
    }

    protected void assertOutputDoesNotContain(String output, String line) {
        assertFalse(
            "Expected the following line not to be in output:\n\n" + line + "\n\nOutput is:\n" + output,
            output.contains(line)
        );
    }

    protected void assertOutputDoesNotContain(String output, String... lines) {
        for (String line : lines) {
            assertOutputDoesNotContain(line);
        }
    }

    protected void assertTaskSuccessfull(BuildResult result, String taskName) {
        BuildTask task = result.task(taskName);
        if (task == null) {
            fail("Expected task `" + taskName + "` to be successful, but it did not run");
        }
        assertEquals(
            "Expected task to be successful but it was: " + task.getOutcome() +
                "\n\nOutput is:\n" + result.getOutput() ,
            TaskOutcome.SUCCESS,
            task.getOutcome()
        );
    }

    protected void assertTaskUpToDate(BuildResult result, String taskName) {
        BuildTask task = result.task(taskName);
        if (task == null) {
            fail("Expected task `" + taskName + "` to be up-to-date, but it did not run");
        }
        assertEquals(
            "Expected task to be up to date but it was: " + task.getOutcome() +
                "\n\nOutput is:\n" + result.getOutput() ,
            TaskOutcome.UP_TO_DATE,
            task.getOutcome()
        );
    }

    protected void assertBuildFileExists(BuildResult result, String projectName, String path) {
        Path absPath = getBuildDir(projectName).toPath().resolve(path);
        assertTrue(
            result.getOutput() + "\n\nExpected `" + absPath + "` to exists but it did not" +
                "\n\nOutput is:\n" + result.getOutput(),
            Files.exists(absPath)
        );
    }

    protected void assertBuildFileDoesNotExists(BuildResult result, String projectName, String path) {
        Path absPath = getBuildDir(projectName).toPath().resolve(path);
        assertFalse(
            result.getOutput() + "\n\nExpected `" + absPath + "` bo to exists but it did" +
                "\n\nOutput is:\n" + result.getOutput(),
            Files.exists(absPath)
        );
    }
}
