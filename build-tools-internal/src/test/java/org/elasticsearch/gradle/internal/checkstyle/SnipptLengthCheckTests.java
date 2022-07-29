/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.checkstyle;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class SnipptLengthCheckTests {

    @Test
    public void testNoSnippets() {
        SnippetLengthCheck.checkFile(failOnError(), 10, "There a no snippets");
    }

    @Test
    public void testEmptySnippet() {
        SnippetLengthCheck.checkFile(failOnError(), 10, "// tag::foo", "// end::foo");
    }

    @Test
    public void testSnippetWithSmallText() {
        SnippetLengthCheck.checkFile(failOnError(), 10, "// tag::foo", "some words", "// end::foo");
    }

    @Test
    public void testSnippetWithLeadingSpaces() {
        SnippetLengthCheck.checkFile(failOnError(), 10, "  // tag::foo", "  some words", "  // end::foo");
    }

    @Test
    public void testSnippetWithEmptyLine() {
        SnippetLengthCheck.checkFile(failOnError(), 10, "  // tag::foo", "", "  some words", "  // end::foo");
    }

    @Test
    public void testSnippetBrokenLeadingSpaces() {
        List<String> collection = new ArrayList<>();
        SnippetLengthCheck.checkFile(collect(collection), 10, "  // tag::foo", "some words", "  // end::foo");
        assertThat(collection, equalTo(singletonList("2: snippet line should start with [  ]")));
    }

    @Test
    public void testSnippetTooLong() {
        List<String> collection = new ArrayList<>();
        SnippetLengthCheck.checkFile(collect(collection), 10, "  // tag::foo", "  too long words", "  // end::foo");
        assertThat(collection, equalTo(singletonList("2: snippet line should be no more than [10] characters but was [14]")));
    }

    @Test
    public void testLotsOfErrors() {
        List<String> collection = new ArrayList<>();
        SnippetLengthCheck.checkFile(collect(collection), 10, "  // tag::foo", "asdfadf", "  too long words", "asdfadf", "  // end::foo");
        assertThat(
            collection,
            equalTo(
                Arrays.asList(
                    "2: snippet line should start with [  ]",
                    "3: snippet line should be no more than [10] characters but was [14]",
                    "4: snippet line should start with [  ]"
                )
            )
        );
    }

    private BiConsumer<Integer, String> failOnError() {
        return (line, message) -> fail("checkstyle error on line [" + line + "] with message [" + message + "]");
    }

    private BiConsumer<Integer, String> collect(List<String> collection) {
        return (line, message) -> collection.add(line + ": " + message);
    }
}
