/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TodoCommentParserTests {

    @Test
    public void testLineComment() {
        List<TodoCommentParser.Hit> hits = TodoCommentParser.find("int x;\n// TODO ship it\n", List.of("TODO", "FIXME"));

        assertThat(hits, contains(new TodoCommentParser.Hit("TODO", "ship it", null, 2)));
    }

    @Test
    public void testAssigneeAndCase() {
        List<TodoCommentParser.Hit> hits = TodoCommentParser.find("// todo(ada): ship it\n", List.of("TODO", "FIXME"));

        assertThat(hits, contains(new TodoCommentParser.Hit("TODO", "ship it", "ada", 1)));
    }

    @Test
    public void testTrailingFixme() {
        List<TodoCommentParser.Hit> hits = TodoCommentParser.find("int x = 1; // FIXME: later\n", List.of("TODO", "FIXME"));

        assertThat(hits, contains(new TodoCommentParser.Hit("FIXME", "later", null, 1)));
    }

    @Test
    public void testBlockComment() {
        List<TodoCommentParser.Hit> hits = TodoCommentParser.find("/* TODO fix this */\n", List.of("TODO", "FIXME"));

        assertThat(hits, contains(new TodoCommentParser.Hit("TODO", "fix this", null, 1)));
    }

    @Test
    public void testJavadocLine() {
        String source = """
            /**
             * FIXME: document this
             */
            """;

        List<TodoCommentParser.Hit> hits = TodoCommentParser.find(source, List.of("TODO", "FIXME"));

        assertThat(hits, contains(new TodoCommentParser.Hit("FIXME", "document this", null, 2)));
    }

    @Test
    public void testMarkerAfterOtherCommentText() {
        String line = "// workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can";

        List<TodoCommentParser.Hit> hits = TodoCommentParser.find(line, List.of("TODO", "FIXME"));

        assertThat(hits, contains(new TodoCommentParser.Hit("TODO", "remove this when we can", null, 1)));
    }

    @Test
    public void testBlockCommentWithTextBeforeMarker() {
        List<TodoCommentParser.Hit> hits = TodoCommentParser.find("/* workaround - FIXME(ada) later */\n", List.of("TODO", "FIXME"));

        assertThat(hits, contains(new TodoCommentParser.Hit("FIXME", "later", "ada", 1)));
    }

    @Test
    public void testInlineBlockCommentStopsAtClose() {
        String line = "BytesRefs.toString(from.fold(FoldContext.small() /* TODO remove me */)).strip()";

        List<TodoCommentParser.Hit> hits = TodoCommentParser.find(line, List.of("TODO", "FIXME"));

        assertThat(hits, contains(new TodoCommentParser.Hit("TODO", "remove me", null, 1)));
    }

    @Test
    public void testIgnoresMarkerInsideAnIdentifierOrString() {
        String source = """
            String s = "TODO";
            String url = "http://TODO";
            int TODOS = 1;
            """;

        assertThat(TodoCommentParser.find(source, List.of("TODO", "FIXME")), empty());
    }

    @Test
    public void testCustomMarker() {
        List<TodoCommentParser.Hit> hits = TodoCommentParser.find("// xxx: later\n", List.of("XXX"));

        assertThat(hits.get(0).marker(), equalTo("XXX"));
        assertThat(hits.get(0).message(), equalTo("later"));
        assertThat(hits.get(0).assignee(), nullValue());
    }

    @Test
    public void testQuotedMarkerInACommentIsNotATask() {
        assertThat(TodoCommentParser.find("// the marker is \"TODO\"\n", List.of("TODO", "FIXME")), empty());
    }

    @Test
    public void testEmptyMarkerListMatchesNothing() {
        assertThat(TodoCommentParser.find("// TODO\n", List.of()), empty());
    }
}
