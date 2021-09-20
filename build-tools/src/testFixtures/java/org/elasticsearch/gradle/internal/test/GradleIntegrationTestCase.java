/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.test;

import org.apache.commons.io.FileUtils;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.BuildTask;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.TaskOutcome;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;

public abstract class GradleIntegrationTestCase extends GradleUnitTestCase {

    @Rule
    public TemporaryFolder testkitTmpDir = new TemporaryFolder();

    public File workingProjectDir = null;

    public abstract String projectName();

    protected File getProjectDir() {
        if (workingProjectDir == null) {
            File root = new File("src/testKit/");
            if (root.exists() == false) {
                throw new RuntimeException(
                    "Could not find resources dir for integration tests. "
                        + "Note that these tests can only be ran by Gradle and are not currently supported by the IDE"
                );
            }
            try {
                workingProjectDir = new File(testkitTmpDir.getRoot(), projectName());
                File sourcFolder = new File(root, projectName());
                FileUtils.copyDirectory(sourcFolder, workingProjectDir);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

        }
        return workingProjectDir;
    }

    protected GradleRunner getGradleRunner() {
        File testkit;
        try {
            testkit = testkitTmpDir.newFolder();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new InternalAwareGradleRunner(GradleRunner.create()
            .withProjectDir(getProjectDir())
            .withPluginClasspath()
            .withTestKitDir(testkit)
            .withDebug(ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0));
    }

    protected File getBuildDir(String name) {
        return new File(getProjectDir(), "build");
    }

    protected void assertOutputContains(String output, String... lines) {
        for (String line : lines) {
            assertOutputContains(output, line);
        }
        List<Integer> index = Stream.of(lines).map(line -> output.indexOf(line)).collect(Collectors.toList());
        if (index.equals(index.stream().sorted().collect(Collectors.toList())) == false) {
            fail(
                "Expected the following lines to appear in this order:\n"
                    + Stream.of(lines).map(line -> "   - `" + line + "`").collect(Collectors.joining("\n"))
                    + "\nTBut the order was different. Output is:\n\n```"
                    + output
                    + "\n```\n"
            );
        }
    }

    protected void assertOutputContains(String output, Set<String> lines) {
        for (String line : lines) {
            assertOutputContains(output, line);
        }
    }

    protected void assertOutputContains(String output, String line) {
        assertThat("Expected the following line in output:\n\n" + line + "\n\nOutput is:\n" + output, output, containsString(line));
    }

    protected void assertOutputMissing(String output, String line) {
        assertFalse("Expected the following line not to be in output:\n\n" + line + "\n\nOutput is:\n" + output, output.contains(line));
    }

    protected void assertOutputMissing(String output, String... lines) {
        for (String line : lines) {
            assertOutputMissing(line);
        }
    }

    protected void assertTaskFailed(BuildResult result, String taskName) {
        assertTaskOutcome(result, taskName, TaskOutcome.FAILED);
    }

    protected void assertTaskSuccessful(BuildResult result, String... taskNames) {
        for (String taskName : taskNames) {
            assertTaskOutcome(result, taskName, TaskOutcome.SUCCESS);
        }
    }

    protected void assertTaskSkipped(BuildResult result, String... taskNames) {
        for (String taskName : taskNames) {
            assertTaskOutcome(result, taskName, TaskOutcome.SKIPPED);
        }
    }

    protected void assertTaskNoSource(BuildResult result, String... taskNames) {
        for (String taskName : taskNames) {
            assertTaskOutcome(result, taskName, TaskOutcome.NO_SOURCE);
        }
    }

    private void assertTaskOutcome(BuildResult result, String taskName, TaskOutcome taskOutcome) {
        BuildTask task = result.task(taskName);
        if (task == null) {
            fail(
                "Expected task `" + taskName + "` to be " + taskOutcome + ", but it did not run" + "\n\nOutput is:\n" + result.getOutput()
            );
        }
        assertEquals(
            "Expected task `"
                + taskName
                + "` to be "
                + taskOutcome
                + " but it was: "
                + task.getOutcome()
                + "\n\nOutput is:\n"
                + result.getOutput(),
            taskOutcome,
            task.getOutcome()
        );
    }

    protected void assertTaskUpToDate(BuildResult result, String... taskNames) {
        for (String taskName : taskNames) {
            BuildTask task = result.task(taskName);
            if (task == null) {
                fail("Expected task `" + taskName + "` to be up-to-date, but it did not run");
            }
            assertEquals(
                "Expected task to be up to date but it was: " + task.getOutcome() + "\n\nOutput is:\n" + result.getOutput(),
                TaskOutcome.UP_TO_DATE,
                task.getOutcome()
            );
        }
    }

    protected void assertNoDeprecationWarning(BuildResult result) {
        assertOutputMissing(result.getOutput(), "Deprecated Gradle features were used in this build");
    }

    protected void assertBuildFileExists(BuildResult result, String projectName, String path) {
        Path absPath = getBuildDir(projectName).toPath().resolve(path);
        assertTrue(
            result.getOutput() + "\n\nExpected `" + absPath + "` to exists but it did not" + "\n\nOutput is:\n" + result.getOutput(),
            Files.exists(absPath)
        );
    }

    protected void assertBuildFileDoesNotExists(BuildResult result, String projectName, String path) {
        Path absPath = getBuildDir(projectName).toPath().resolve(path);
        assertFalse(
            result.getOutput() + "\n\nExpected `" + absPath + "` bo to exists but it did" + "\n\nOutput is:\n" + result.getOutput(),
            Files.exists(absPath)
        );
    }

    protected String getLocalTestDownloadsPath() {
        return getLocalTestPath("test.local-test-downloads-path");
    }

    private String getLocalTestPath(String propertyName) {
        String property = System.getProperty(propertyName);
        Objects.requireNonNull(property, propertyName + " not passed to tests");
        File file = new File(property);
        assertTrue("Expected " + property + " to exist, but it did not!", file.exists());
        if (File.separator.equals("\\")) {
            // Use / on Windows too, the build script is not happy with \
            return file.getAbsolutePath().replace(File.separator, "/");
        } else {
            return file.getAbsolutePath();
        }
    }

    public void assertOutputOnlyOnce(String output, String... text) {
        for (String each : text) {
            int i = output.indexOf(each);
            if (i == -1) {
                fail("Expected \n```" + each + "```\nto appear at most once, but it didn't at all.\n\nOutout is:\n" + output);
            }
            if (output.indexOf(each) != output.lastIndexOf(each)) {
                fail("Expected `" + each + "` to appear at most once, but it did multiple times.\n\nOutout is:\n" + output);
            }
        }
    }

}
