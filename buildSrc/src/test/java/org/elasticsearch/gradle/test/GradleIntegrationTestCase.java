package org.elasticsearch.gradle.test;

import org.gradle.testkit.runner.GradleRunner;

import java.io.File;
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
        return new File(root, name);
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

}
