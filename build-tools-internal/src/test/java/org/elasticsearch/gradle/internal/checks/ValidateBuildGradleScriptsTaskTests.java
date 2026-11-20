/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.checks;

import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ValidateBuildGradleScriptsTaskTests {

    @Test
    public void testSucceedsForDependencyProjectNotation() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "subproject/build.gradle", List.of("dependencies {", "  implementation project(\":server\")", "}"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testFailsForCrossProjectDereference() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "distribution/tools/plugin-cli/bc/build.gradle",
            List.of("tasks.named(\"forbiddenApisMain\").configure {", "  classpath += project(\":server\").sourceSets.main.runtimeClasspath", "}")
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("cross-project-dereference"));
            assertTrue(e.getMessage().contains("distribution/tools/plugin-cli/bc/build.gradle:2"));
            assertTrue(e.getMessage().contains("project(\":server\").sourceSets.main.runtimeClasspath"));
        }
    }

    @Test
    public void testFailsForCrossProjectConfigurationClosure() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "distribution/build.gradle", List.of("project(\":server\") {", "  version = version", "}"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("cross-project-configuration"));
            assertTrue(e.getMessage().contains("distribution/build.gradle:1"));
        }
    }

    @Test
    public void testIgnoreSuppressesViolation() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "distribution/tools/plugin-cli/bc/build.gradle",
            List.of("tasks.named(\"forbiddenApisMain\").configure {", "  classpath += project(\":server\").sourceSets.main.runtimeClasspath", "}")
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("cross-project-dereference", List.of("distribution/tools/plugin-cli/bc/build.gradle")));

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testFailsForDirectBundledJdkVersionUseInDistributionScript() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "distribution/archives/build.gradle",
            List.of("if (Integer.parseInt(VersionProperties.bundledJdkMajorVersion) < 27) {", "  println 'x'", "}")
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("distribution-bundled-jdk-source-of-truth"));
            assertTrue(e.getMessage().contains("distribution/archives/build.gradle:1"));
        }
    }

    @Test
    public void testFailsForClosureAssignmentInDistributionScript() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "distribution/build.gradle", List.of("def architecturesFor = { String platform -> ['x64'] }"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("distribution-closure-assignment"));
            assertTrue(e.getMessage().contains("distribution/build.gradle:1"));
        }
    }

    @Test
    public void testAllowsClosureAssignmentOutsideDistributionScripts() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "x-pack/build.gradle", List.of("def helper = { String name -> name }"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testFailsForExtClosureAssignmentInDistributionScript() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "distribution/docker/build.gradle", List.of("ext.expansions = { Architecture architecture -> [:] }"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("distribution-ext-closure-api"));
            assertTrue(e.getMessage().contains("distribution/docker/build.gradle:1"));
        }
    }

    @Test
    public void testAllowsExtClosureAssignmentOutsideDistributionScripts() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "x-pack/build.gradle", List.of("ext.helper = { String name -> name }"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testFailsForEagerTestSeedReads() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "x-pack/plugin/foo/qa/s3/build.gradle",
            List.of(
                "def seed = buildParams.testSeedProvider.get()",
                "systemProperty 'base', buildParams.getTestSeedProvider().map { s -> 'base_' + s }.get()",
                "systemProperty 'flag', buildParams.random.map{r -> r.nextBoolean()}.get()",
                "def random = buildParams.getRandom().getOrNull()",
                "def other = buildParams.random.getOrElse(new Random())",
                "def orNull = buildParams.testSeedProvider.orNull",
                "def chained = buildParams.testSeedProvider",
                "  .map { s -> 'base_' + s }",
                "  .get()",
                "def gstring = buildParams.testSeedProvider.map { \"seed_${it}\" }.get()"
            )
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            for (int line : List.of(1, 2, 3, 4, 5, 6, 7, 10)) {
                assertTrue(e.getMessage(), e.getMessage().contains("[eager-test-seed] x-pack/plugin/foo/qa/s3/build.gradle:" + line));
            }
        }
    }

    /**
     * A long property chain with no read must fail to match quickly; whitespace that two parts of the pattern could both
     * match made this take exponential time.
     */
    @Test(timeout = 10_000)
    public void testEagerTestSeedRuleDoesNotBacktrackOnLongChains() throws Exception {
        Project project = createProject();
        List<String> lines = new ArrayList<>();
        lines.add("def x = buildParams.random");
        for (int i = 0; i < 30; i++) {
            lines.add("        .p" + i);
        }
        lines.add("println x");
        File scriptFile = writeFile(project, "x-pack/plugin/foo/build.gradle", lines);
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testAllowsLazyTestSeedUse() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "x-pack/plugin/foo/qa/s3/build.gradle",
            List.of(
                "nonInputProperties.systemProperty 'base', buildParams.testSeedProvider.map { seed -> 'base_' + seed }",
                "nonInputProperties.systemProperty 'flag', buildParams.random.map{r -> r.nextBoolean()}",
                "nonInputProperties.systemProperty 'path', buildParams.testSeedProvider.map { s -> basePath.get() + s }",
                "nonInputProperties.systemProperty 'n', buildParams.random.map { r -> r.nextInt(10) }.map { n -> names.getOrElse(n, 'x') }",
                "def bucket = providers.environmentVariable('BUCKET').get()"
            )
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testCountedIgnoreSuppressesExactlyTheBaselinedNumberOfViolations() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "distribution/build.gradle",
            List.of("def one = { }", "def two = { }")
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("distribution-closure-assignment", List.of("distribution/build.gradle::2")));

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testCountedIgnoreFailsWhenAdditionalViolationIsAddedToBaselinedFile() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "distribution/build.gradle",
            List.of("def one = { }", "def two = { }", "def three = { }")
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("distribution-closure-assignment", List.of("distribution/build.gradle::2")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("distribution-closure-assignment"));
            assertTrue(e.getMessage().contains("distribution/build.gradle:3"));
        }
    }

    @Test
    public void testCountedIgnoreFailsWhenBaselineCountBecomesStale() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "distribution/build.gradle", List.of("def one = { }"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("distribution-closure-assignment", List.of("distribution/build.gradle::2")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("stale-ignore-entry"));
            assertTrue(e.getMessage().contains("distribution-closure-assignment -> distribution/build.gradle::2"));
        }
    }

    @Test
    public void testFailsForStaleIgnoreEntry() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "subproject/build.gradle", List.of("dependencies {", "  implementation project(\":server\")", "}"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("cross-project-dereference", List.of("subproject/build.gradle")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("stale-ignore-entry"));
            assertTrue(e.getMessage().contains("cross-project-dereference -> subproject/build.gradle"));
        }
    }

    @Test
    public void testInvalidIgnoreFormatFailsClearly() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "subproject/build.gradle", List.of("dependencies {", "  implementation project(\":server\")", "}"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("", List.of("subproject/build.gradle")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("Ignore rule ids must not be blank"));
        }
    }

    @Test
    public void testBlankIgnorePathFailsClearly() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "subproject/build.gradle", List.of("dependencies {", "  implementation project(\":server\")", "}"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("cross-project-dereference", List.of("  ")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("Ignore path for rule [cross-project-dereference] must not be blank"));
        }
    }

    @Test
    public void testInvalidIgnoreCountFailsClearly() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(project, "distribution/build.gradle", List.of("def one = { }"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);
        task.getIgnore().set(java.util.Map.of("distribution-closure-assignment", List.of("distribution/build.gradle::abc")));

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("Ignore count for rule [distribution-closure-assignment] must be an integer"));
        }
    }

    @Test
    public void testExecutionClosureBaselineLifecycle() throws Exception {
        Project project = createProject();
        File script = writeFile(project, "subproject/build.gradle", List.of("tasks.register('probe') {", "  onlyIf { project.name }", "}"));
        ValidateBuildGradleScriptsTask task = createTask(project, script);
        task.getIgnore().set(java.util.Map.of(ExecutionClosureScanner.RULE_ID, List.of("subproject/build.gradle::1")));
        task.validateScripts();
        assertOutputMarkerWritten(task);

        Files.writeString(script.toPath(), "tasks.register('probe') { onlyIf { project.name }; doLast { gradle.startParameter } }\n");
        try {
            task.validateScripts();
            fail("Added execution reference must fail");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains(ExecutionClosureScanner.RULE_ID));
            assertTrue(e.getMessage().contains("gradle"));
        }

        Files.writeString(script.toPath(), "tasks.register('probe') { onlyIf { true } }\n");
        try {
            task.validateScripts();
            fail("Removed execution reference must make the baseline stale");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("stale-ignore-entry"));
        }
    }

    @Test
    public void testLegacyTaskExecutionReferencesAreRejected() throws Exception {
        for (String declaration : List.of("task probe", "task probe(type: Exec)", "task('probe')")) {
            Project project = createProject();
            File script = writeFile(project, "subproject/build.gradle", List.of(declaration + " {", "  doLast { project.name }", "}"));
            ValidateBuildGradleScriptsTask task = createTask(project, script);
            try {
                task.validateScripts();
                fail("Expected a model reference finding for " + declaration);
            } catch (GradleException e) {
                assertTrue(e.getMessage().contains(ExecutionClosureScanner.RULE_ID));
                assertTrue(e.getMessage().contains("subproject/build.gradle:2"));
            }
        }
    }

    @Test
    public void testLegacyTaskConfigurationCaptureIsAllowed() throws Exception {
        for (String declaration : List.of("task probe", "task probe(type: Exec)", "task('probe')")) {
            Project project = createProject();
            File script = writeFile(
                project,
                "subproject/build.gradle",
                List.of(declaration + " {", "  def name = project.name", "  doLast { println name }", "}")
            );
            ValidateBuildGradleScriptsTask task = createTask(project, script);
            task.validateScripts();
            assertOutputMarkerWritten(task);
        }
    }

    @Test
    public void testQualifiedModelAccessConsumesOneIgnorePerReceiver() throws Exception {
        Project project = createProject();
        File script = writeFile(
            project,
            "subproject/build.gradle",
            List.of("tasks.register('probe') { doLast { project.file('data'); project.sourceSets.main.output } }")
        );
        ValidateBuildGradleScriptsTask task = createTask(project, script);
        task.getIgnore().set(java.util.Map.of(ExecutionClosureScanner.RULE_ID, List.of("subproject/build.gradle::2")));
        task.validateScripts();
        assertOutputMarkerWritten(task);
    }

    @Test
    public void testParseFailureIsActionableAndCannotBeIgnored() throws Exception {
        Project project = createProject();
        File script = writeFile(project, "subproject/build.gradle", List.of("tasks.register('broken') {"));
        ValidateBuildGradleScriptsTask task = createTask(project, script);
        task.getIgnore().set(java.util.Map.of("build-script-parse-failure", List.of("subproject/build.gradle")));
        try {
            task.validateScripts();
            fail("Unparsed scripts cannot pass validation");
        } catch (GradleException e) {
            assertTrue(e.getMessage().contains("build-script-parse-failure"));
            assertTrue(e.getMessage().contains("subproject/build.gradle"));
            assertTrue(e.getMessage().contains("Unexpected input"));
        }
    }

    @Test
    public void testFailsForAntUsageInAnyGradleScript() throws Exception {
        Project project = createProject();
        File buildScript = writeFile(
            project,
            "subproject/build.gradle",
            List.of(
                "import org.apache.tools.ant.filters.ReplaceTokens",
                "tasks.register('regen') {",
                "  doLast {",
                "    ant.move(file: 'a', toDir: 'b')",
                "    project.ant { echo(message: 'x') }",
                "  }",
                "}"
            )
        );
        File scriptPlugin = writeFile(
            project,
            "build-tools-internal/src/main/groovy/elasticsearch.example.gradle",
            List.of("def builder = createAntBuilder()", "files('a').addToAntBuilder(builder, 'fileset')", "getAnt().echo('x')")
        );
        File settings = writeFile(project, "settings.gradle", List.of("ant.properties.foo = 'bar'"));
        ValidateBuildGradleScriptsTask task = createTask(project, buildScript, scriptPlugin, settings);

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            String message = e.getMessage();
            assertTrue(message.contains("[ant-usage] subproject/build.gradle:1"));
            assertTrue(message.contains("[ant-usage] subproject/build.gradle:4"));
            assertTrue(message.contains("[ant-usage] subproject/build.gradle:5"));
            assertTrue(message.contains("[ant-usage] build-tools-internal/src/main/groovy/elasticsearch.example.gradle:1"));
            assertTrue(message.contains("[ant-usage] build-tools-internal/src/main/groovy/elasticsearch.example.gradle:2"));
            assertTrue(message.contains("[ant-usage] build-tools-internal/src/main/groovy/elasticsearch.example.gradle:3"));
            assertTrue(message.contains("[ant-usage] settings.gradle:1"));
            assertEquals(7, message.lines().filter(l -> l.startsWith("- [ant-usage]")).count());
        }
    }

    @Test
    public void testAntUsageRuleFlagsAntPropertyAndDeprecatedTypes() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "subproject/build.gradle",
            List.of(
                "def builder = project.ant",
                "def other = rootProject.ant",
                "class Foo implements AntBuilderAware {}",
                "FileCollection.AntType type = null"
            )
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            String message = e.getMessage();
            assertEquals(4, message.lines().filter(l -> l.startsWith("- [ant-usage]")).count());
        }
    }

    @Test
    public void testAntUsageRuleIgnoresInlineComments() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "subproject/build.gradle",
            List.of(
                "def value = 'x' // use Copy instead of ant.copy",
                "def other = 'y' /* ant.move(file: 'a') */",
                "def url = 'http://example.com/index.html'",
                "/*",
                " ant.echo(message: 'x')",
                "*/"
            )
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testAntUsageRuleIgnoresStringLiterals() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "subproject/build.gradle",
            List.of(
                "tasks.register('x') { description = 'Replaces the former project.ant based task' }",
                "def a = \"uses ant.copy and getAnt() in text\"",
                "def b = '''",
                "  project.ant { echo(message: 'x') }",
                "'''",
                "def c = $/ant.move(file: 'a') and a $$ and $/ escape/$"
            )
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testAntUsageRuleChecksCodeAfterCommentMarkersInStrings() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "subproject/build.gradle",
            List.of(
                "def glob = $/src/*.txt/$",
                "ant.echo(message: 'x')",
                "def url = 'http://example.com/*'",
                "def message = \"${project.ant.properties['x']}\""
            )
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        try {
            task.validateScripts();
            fail("GradleException was expected");
        } catch (GradleException e) {
            String message = e.getMessage();
            assertTrue(message.contains("[ant-usage] subproject/build.gradle:2"));
            assertTrue(message.contains("[ant-usage] subproject/build.gradle:4"));
            assertEquals(2, message.lines().filter(l -> l.startsWith("- [ant-usage]")).count());
        }
    }

    @Test
    public void testEagerTestSeedRuleOnlyAppliesToBuildScripts() throws Exception {
        Project project = createProject();
        File scriptPlugin = writeFile(
            project,
            "build-tools-internal/src/main/groovy/elasticsearch.example.gradle",
            List.of("def seed = buildParams.testSeedProvider.get()")
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptPlugin);

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testAntUsageRuleIgnoresCommentsAndLookalikes() throws Exception {
        Project project = createProject();
        File scriptFile = writeFile(
            project,
            "subproject/build.gradle",
            List.of(
                "// Use a plain file edit rather than ant.replace: Task.getAnt() is deprecated",
                "/* ant.move(file: 'a') */",
                "/**",
                " * ant.fixcrlf in a javadoc-style comment",
                " */",
                "def variant = 'x'",
                "variant.toString()",
                "def constant = 1",
                "dependencies { implementation buildLibs.ant }",
                "import org.elasticsearch.gradle.internal.conventions.filters.ReplaceTokens",
                "def description = 'an ant colony'"
            )
        );
        ValidateBuildGradleScriptsTask task = createTask(project, scriptFile);

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    @Test
    public void testBuildScriptOnlyRulesSkipOtherGradleScripts() throws Exception {
        Project project = createProject();
        // cross-project access and execution-time project references are only validated in build.gradle files
        File scriptPlugin = writeFile(
            project,
            "build-tools-internal/src/main/groovy/elasticsearch.example.gradle",
            List.of("def v = project(':server').version", "tasks.register('x') { doLast { println project.version } }")
        );
        File distributionSettings = writeFile(project, "distribution/settings.gradle", List.of("ext.helper = { -> 1 }"));
        ValidateBuildGradleScriptsTask task = createTask(project, scriptPlugin, distributionSettings);

        task.validateScripts();

        assertOutputMarkerWritten(task);
    }

    private Project createProject() throws IOException {
        File projectDir = Files.createTempDirectory("validate-build-gradle-scripts-task").toFile();
        return ProjectBuilder.builder().withProjectDir(projectDir).build();
    }

    private ValidateBuildGradleScriptsTask createTask(Project project, File... scriptFiles) {
        ValidateBuildGradleScriptsTask task = project.getTasks().create("validateBuildGradleScripts", ValidateBuildGradleScriptsTask.class);
        task.getScriptFiles().from((Object[]) scriptFiles);
        return task;
    }

    private static File writeFile(Project project, String relativePath, List<String> lines) throws IOException {
        File file = new File(project.getProjectDir(), relativePath);
        file.getParentFile().mkdirs();
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
        return file;
    }

    private static void assertOutputMarkerWritten(ValidateBuildGradleScriptsTask task) throws IOException {
        File marker = task.getOutputMarker();
        assertTrue(marker.exists());
        assertEquals("done", Files.readString(marker.toPath(), StandardCharsets.UTF_8));
    }
}
