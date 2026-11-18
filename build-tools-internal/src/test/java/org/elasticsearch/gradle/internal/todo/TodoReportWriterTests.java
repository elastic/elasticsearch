/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TodoReportWriterTests {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testMergeSortsRowsAndWritesCsv() throws Exception {
        Path dir = temp.getRoot().toPath();
        TodoEntry later = entry("FIXME", "later", null, "b.java", ":server", "server", "main", 9);
        TodoEntry earlier = entry("TODO", "say \"hi\", now", "ada", "a.java", ":modules:foo", "modules", "test", 3);
        TodoReportWriter.writeJson(dir.resolve("one.json"), List.of(later));
        TodoReportWriter.writeJson(dir.resolve("two.json"), List.of(earlier));

        List<TodoEntry> merged = TodoReportWriter.merge(List.of(dir.resolve("one.json"), dir.resolve("two.json")));
        Path csv = dir.resolve("todos.csv");
        TodoReportWriter.writeCsv(csv, merged);

        assertThat(merged, hasSize(2));
        assertThat(merged.get(0).module(), equalTo(":modules:foo"));
        assertThat(merged.get(1).file(), equalTo("b.java"));
        String text = Files.readString(csv);
        assertThat(text, containsString("marker,message,assignee,file,module,area,sourceSet,line,"));
        assertThat(text, containsString("\"say \"\"hi\"\", now\""));
        assertThat(text, containsString(":modules:foo"));
    }

    @Test
    public void testLocations() {
        Path root = Path.of("/repo");
        Path file = root.resolve("x-pack/plugin/esql/src/test/java/Foo.java");

        assertThat(TodoLocations.area(":x-pack:plugin:esql"), equalTo("x-pack"));
        assertThat(TodoLocations.area(":server"), equalTo("server"));
        assertThat(TodoLocations.sourceSet(root.resolve("x-pack/plugin/esql"), file), equalTo("test"));
        assertThat(TodoLocations.repoRelative(root, file), equalTo("x-pack/plugin/esql/src/test/java/Foo.java"));
        assertThat(TodoLocations.isUnderBuild(root.resolve("server"), root.resolve("server/build/generated/Foo.java")), equalTo(true));
    }

    private static TodoEntry entry(
        String marker,
        String message,
        String assignee,
        String file,
        String module,
        String area,
        String sourceSet,
        int line
    ) {
        return new TodoEntry(marker, message, assignee, file, module, area, sourceSet, line, "Ada", "ada@example.com", "t", "d", "abc", "ok");
    }
}
