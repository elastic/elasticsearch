/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NoticeTaskTests {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    /**
     * Notices of {@code @notice} annotated sources are listed by file name, regardless of the order the source
     * directories were registered in or the order the file system returns their files in.
     */
    @Test
    public void sourceNoticesAreOrderedByFileName() throws IOException {
        File first = sourceDir("first", "Zeta.java", "Alpha.java");
        File second = sourceDir("second", "Beta.java", "Alpha.java");

        String forward = generateNotice("forward", first, second);
        String reversed = generateNotice("reversed", second, first);

        assertEquals(forward, reversed);
        assertOrdered(
            forward,
            "first/Alpha.java notice",
            "second/Alpha.java notice",
            "second/Beta.java notice",
            "first/Zeta.java notice"
        );
    }

    @Test
    public void sortedSourceFilesBreaksNameTiesByPath() throws IOException {
        File first = sourceDir("first", "Same.java");
        File second = sourceDir("second", "Same.java");
        Project project = ProjectBuilder.builder().withProjectDir(temporaryFolder.newFolder("project")).build();

        List<File> sorted = NoticeTask.sortedSourceFiles(
            project.files(new File(second, "Same.java"), new File(first, "Same.java")),
            temporaryFolder.getRoot()
        );

        assertEquals(List.of(new File(first, "Same.java"), new File(second, "Same.java")), sorted);
    }

    /**
     * Ties are broken on a path that is independent of the checkout location and the platform's path separator, which
     * would otherwise order e.g. {@code x/Same.java} and {@code xA/Same.java} differently on Windows and Unix.
     */
    @Test
    public void portablePathIsRelativeAndSlashSeparated() {
        File base = temporaryFolder.getRoot();
        File file = new File(new File(new File(base, "a"), "b"), "Same.java");
        assertEquals("a/b/Same.java", NoticeTask.portablePath(base, file));
        assertTrue(NoticeTask.portablePath(base, new File(new File(base, "x"), "Same.java")).compareTo(
            NoticeTask.portablePath(base, new File(new File(base, "xA"), "Same.java"))
        ) < 0);
    }

    private String generateNotice(String name, File... sourceDirs) throws IOException {
        Project project = ProjectBuilder.builder().withProjectDir(temporaryFolder.newFolder(name)).build();
        NoticeTask task = project.getTasks().register("notice", NoticeTask.class).get();
        File inputFile = temporaryFolder.newFile(name + "-NOTICE.txt");
        Files.writeString(inputFile.toPath(), "Elasticsearch\n", StandardCharsets.UTF_8);
        task.setInputFile(inputFile);
        for (File sourceDir : sourceDirs) {
            task.source(sourceDir);
        }
        task.generateNotice();
        return Files.readString(task.getOutputFile().toPath(), StandardCharsets.UTF_8);
    }

    private File sourceDir(String name, String... fileNames) throws IOException {
        File dir = temporaryFolder.newFolder(name);
        for (String fileName : fileNames) {
            Files.writeString(dir.toPath().resolve(fileName), """
                /*
                 * @notice
                 * %s/%s notice
                 */
                class Dummy {}
                """.formatted(name, fileName), StandardCharsets.UTF_8);
        }
        return dir;
    }

    private static void assertOrdered(String text, String... expected) {
        int previous = -1;
        for (String e : expected) {
            int index = text.indexOf(e);
            assertTrue("expected [" + e + "] after index [" + previous + "] in:\n" + text, index > previous);
            previous = index;
        }
    }
}
