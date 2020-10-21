/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle.test;

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

    protected File getProjectDir(String name) {
        File root = new File("src/testKit/");
        if (root.exists() == false) {
            throw new RuntimeException(
                "Could not find resources dir for integration tests. "
                    + "Note that these tests can only be ran by Gradle and are not currently supported by the IDE"
            );
        }
        return new File(root, name).getAbsoluteFile();
    }

    protected GradleRunner getGradleRunner(String sampleProject) {
        File testkit;
        try {
            testkit = testkitTmpDir.newFolder();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return GradleRunner.create()
            .withProjectDir(getProjectDir(sampleProject))
            .withPluginClasspath()
            .withTestKitDir(testkit)
            .withDebug(ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0);
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

    protected void assertOutputDoesNotContain(String output, String line) {
        assertFalse("Expected the following line not to be in output:\n\n" + line + "\n\nOutput is:\n" + output, output.contains(line));
    }

    protected void assertOutputDoesNotContain(String output, String... lines) {
        for (String line : lines) {
            assertOutputDoesNotContain(line);
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
        assertOutputDoesNotContain(result.getOutput(), "Deprecated Gradle features were used in this build");
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
