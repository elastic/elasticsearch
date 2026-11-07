/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo;

import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class MergeTodoReportTaskTests {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testWritesMergedJsonAndCsv() throws Exception {
        Project project = ProjectBuilder.builder().withProjectDir(temp.getRoot()).build();
        Path dir = temp.getRoot().toPath();
        TodoReportWriter.writeJson(dir.resolve("a.json"), List.of(entry("b.java", ":server", 4)));
        TodoReportWriter.writeJson(dir.resolve("b.json"), List.of(entry("a.java", ":modules:foo", 2)));

        MergeTodoReportTask task = project.getTasks().register("todoReport", MergeTodoReportTask.class).get();
        task.getReportFiles().from(dir.resolve("a.json").toFile(), dir.resolve("b.json").toFile());
        task.getOutputJson().set(dir.resolve("todos.json").toFile());
        task.getOutputCsv().set(dir.resolve("todos.csv").toFile());
        task.merge();

        TodoReportFile report = TodoReportWriter.read(dir.resolve("todos.json"));
        assertThat(report.items().get(0).file(), equalTo("a.java"));
        assertThat(report.items().get(1).module(), equalTo(":server"));
        assertThat(java.nio.file.Files.readString(dir.resolve("todos.csv")).contains("a.java,modules"), equalTo(false));
        assertThat(java.nio.file.Files.readString(dir.resolve("todos.csv")).contains(":modules:foo"), equalTo(true));
    }

    private static TodoEntry entry(String file, String module, int line) {
        String area = module.substring(1, module.indexOf(':', 1) < 0 ? module.length() : module.indexOf(':', 1));
        return new TodoEntry("TODO", "note", null, file, module, area, "main", line, "Ada", "ada@example.com", "t", "d", "abc", "ok");
    }
}
