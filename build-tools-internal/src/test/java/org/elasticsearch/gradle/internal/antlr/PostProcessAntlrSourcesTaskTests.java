/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.antlr;

import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class PostProcessAntlrSourcesTaskTests {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void replaceFirstOnlyReplacesFirstMatch() {
        String content = "public class FooBaseLexer {}\npublic class FooBaseParser {}\n";
        Pattern pattern = Pattern.compile("public ((interface|class) \\QFooBase\\E\\w+)");
        assertEquals(
            "class FooBaseLexer {}\npublic class FooBaseParser {}\n",
            PostProcessAntlrSourcesTask.applyEdit(content, pattern, "$1", false)
        );
    }

    @Test
    public void replaceAllReplacesEveryMatchWithBackReferences() {
        String content = "  public FooContext(int a) {}\n    public BarContext() {}\n";
        Pattern pattern = Pattern.compile("([ ]+)public ([A-Z][a-z]+[a-z,A-Z]+\\()");
        assertEquals(
            "  @SuppressWarnings(\"this-escape\")\n  public FooContext(int a) {}\n"
                + "    @SuppressWarnings(\"this-escape\")\n    public BarContext() {}\n",
            PostProcessAntlrSourcesTask.applyEdit(content, pattern, "$1@SuppressWarnings(\"this-escape\")\n$1public $2", true)
        );
    }

    @Test
    public void dotDoesNotMatchLineBreaks() {
        String content = "// Generated from /tmp/Foo.g4 by ANTLR 4.13\npackage foo;\n";
        Pattern pattern = Pattern.compile("\\Q// Generated from \\E.*");
        assertEquals(
            "// ANTLR GENERATED CODE: DO NOT EDIT\npackage foo;\n",
            PostProcessAntlrSourcesTask.applyEdit(content, pattern, "// ANTLR GENERATED CODE: DO NOT EDIT", false)
        );
    }

    @Test
    public void normalizesLineEndings() {
        assertEquals("a\nb\nc\n", PostProcessAntlrSourcesTask.normalizeLineEndings("a\r\nb\rc"));
        assertEquals("a\n", PostProcessAntlrSourcesTask.normalizeLineEndings("a\n"));
        assertEquals("", PostProcessAntlrSourcesTask.normalizeLineEndings(""));
    }

    @Test
    public void postProcessesGeneratedSources() throws IOException {
        File projectDir = temporaryFolder.newFolder("project");
        Path outputDir = Files.createDirectories(projectDir.toPath().resolve("src/main/java/foo"));
        Path grammarDir = projectDir.toPath().resolve("src/main/antlr");
        Files.writeString(outputDir.resolve("FooBase.tokens"), "A=1\n");
        Files.writeString(outputDir.resolve("FooBaseLexer.tokens"), "B=2\n");
        Files.writeString(
            outputDir.resolve("FooBaseLexer.java"),
            "// Generated from /some/path/FooBase.g4 by ANTLR\r\npublic class FooBaseLexer {\r\n\tint a;\r\n\tint b;\r\n}"
        );
        Files.writeString(outputDir.resolve("Other.java"), "public class Other {\n\tint a;\n}\n");

        Project project = ProjectBuilder.builder().withProjectDir(projectDir).build();
        PostProcessAntlrSourcesTask task = project.getTasks().register("regen", PostProcessAntlrSourcesTask.class, t -> {
            t.getGeneratedSourcesDir().set(outputDir.toFile());
            t.getTokensDir().set(grammarDir.toFile());
            t.getTokenFiles().set(List.of("FooBase.tokens", "FooBaseLexer.tokens"));
            t.replaceFirst("FooBase*.java", "public ((interface|class) \\QFooBase\\E\\w+)", "$1");
            t.replaceFirst("FooBase*.java", "\\Q// Generated from \\E.*", "// ANTLR GENERATED CODE: DO NOT EDIT");
            t.replaceAll("FooBase*.java", "\t", "  ");
            t.fixLineEndings("FooBase*.java");
        }).get();

        task.postProcess();

        assertFalse(Files.exists(outputDir.resolve("FooBase.tokens")));
        assertEquals("A=1\n", Files.readString(grammarDir.resolve("FooBase.tokens")));
        assertEquals("B=2\n", Files.readString(grammarDir.resolve("FooBaseLexer.tokens")));
        assertEquals(
            "// ANTLR GENERATED CODE: DO NOT EDIT\nclass FooBaseLexer {\n  int a;\n  int b;\n}\n",
            Files.readString(outputDir.resolve("FooBaseLexer.java"), StandardCharsets.UTF_8)
        );
        // files not matching the include are left untouched
        assertEquals("public class Other {\n\tint a;\n}\n", Files.readString(outputDir.resolve("Other.java")));
    }

    @Test
    public void failsWhenTokenFileIsMissing() throws IOException {
        File projectDir = temporaryFolder.newFolder("project");
        Path outputDir = Files.createDirectories(projectDir.toPath().resolve("out"));

        Project project = ProjectBuilder.builder().withProjectDir(projectDir).build();
        PostProcessAntlrSourcesTask task = project.getTasks().register("regen", PostProcessAntlrSourcesTask.class, t -> {
            t.getGeneratedSourcesDir().set(outputDir.toFile());
            t.getTokensDir().set(projectDir);
            t.getTokenFiles().set(List.of("Missing.tokens"));
        }).get();

        GradleException e = assertThrows(GradleException.class, task::postProcess);
        assertTrue(e.getMessage().contains("Missing.tokens"));
    }

    @Test
    public void failsWhenIncludeMatchesNoFiles() throws IOException {
        File projectDir = temporaryFolder.newFolder("project");
        Path outputDir = Files.createDirectories(projectDir.toPath().resolve("out"));
        Files.writeString(outputDir.resolve("Foo.java"), "class Foo {}\n");

        Project project = ProjectBuilder.builder().withProjectDir(projectDir).build();
        PostProcessAntlrSourcesTask task = project.getTasks().register("regen", PostProcessAntlrSourcesTask.class, t -> {
            t.getGeneratedSourcesDir().set(outputDir.toFile());
            t.replaceAll("Typo*.java", "\t", "  ");
        }).get();

        GradleException e = assertThrows(GradleException.class, task::postProcess);
        assertTrue(e.getMessage().contains("Typo*.java"));
    }
}
