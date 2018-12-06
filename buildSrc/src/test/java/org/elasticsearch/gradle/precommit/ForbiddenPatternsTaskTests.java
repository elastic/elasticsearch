package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ForbiddenPatternsTaskTests extends GradleUnitTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public void testCheckInvalidPatternsWhenNoSourceFilesExist() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);

        task.checkInvalidPatterns();

        File outputMarker = new File(project.getBuildDir(), "markers/forbiddenPatterns");
        Optional<String> result = Files.readAllLines(outputMarker.toPath(), StandardCharsets.UTF_8).stream().findFirst();

        assertTrue(result.isPresent());
        assertEquals("done", result.get());
    }

    public void testCheckInvalidPatternsWhenSourceFilesExistNoViolation() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);

        String line = "public void bar() {}";
        File file = new File(project.getProjectDir(), "src/main/java/Foo.java");
        file.getParentFile().mkdirs();
        file.createNewFile();
        Files.write(file.toPath(), line.getBytes());

        task.checkInvalidPatterns();

        File outputMarker = new File(project.getBuildDir(), "markers/forbiddenPatterns");
        Optional<String> result = Files.readAllLines(outputMarker.toPath(), StandardCharsets.UTF_8).stream().findFirst();

        assertTrue(result.isPresent());
        assertEquals("done", result.get());
    }

    public void testCheckInvalidPatternsWhenSourceFilesExistHavingTab() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);

        String line = "\tpublic void bar() {}";
        File file = new File(project.getProjectDir(), "src/main/java/Bar.java");
        file.getParentFile().mkdirs();
        file.createNewFile();
        Files.write(file.toPath(), line.getBytes());

        try {
            task.checkInvalidPatterns();
            fail("GradleException was expected to be thrown in this case!");
        } catch (GradleException e) {
            assertTrue(e.getMessage().startsWith("Found invalid patterns"));
        }
    }

    public void testCheckInvalidPatternsWithCustomRule() throws Exception {
        Map<String, String> rule = new HashMap<>();
        rule.put("name", "TODO comments are not allowed");
        rule.put("pattern", "\\/\\/.*(?i)TODO");

        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);
        task.rule(rule);

        List<String> lines = Arrays.asList("GOOD LINE", "//todo", "// some stuff, toDo");
        File file = new File(project.getProjectDir(), "src/main/java/Moot.java");
        file.getParentFile().mkdirs();
        file.createNewFile();
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        try {
            task.checkInvalidPatterns();
            fail("GradleException was expected to be thrown in this case!");
        } catch (GradleException e) {
            assertTrue(e.getMessage().startsWith("Found invalid patterns"));
        }
    }

    public void testCheckInvalidPatternsWhenExcludingFiles() throws Exception {
        Project project = createProject();
        ForbiddenPatternsTask task = createTask(project);
        task.exclude("**/*.java");

        File file = new File(project.getProjectDir(), "src/main/java/FooBarMoot.java");
        file.getParentFile().mkdirs();
        file.createNewFile();
        Files.write(file.toPath(), "\t".getBytes());

        task.checkInvalidPatterns();

        File outputMarker = new File(project.getBuildDir(), "markers/forbiddenPatterns");
        Optional<String> result = Files.readAllLines(outputMarker.toPath(), StandardCharsets.UTF_8).stream().findFirst();

        assertTrue(result.isPresent());
        assertEquals("done", result.get());
    }

    private Project createProject() {
        Project project = ProjectBuilder.builder().build();
        project.getPlugins().apply(JavaPlugin.class);

        return project;
    }

    private ForbiddenPatternsTask createTask(Project project) {
        return project.getTasks().create("forbiddenPatterns", ForbiddenPatternsTask.class);
    }
}
