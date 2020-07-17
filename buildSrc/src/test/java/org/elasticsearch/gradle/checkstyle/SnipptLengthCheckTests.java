/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.checkstyle;

import org.elasticsearch.gradle.test.GradleUnitTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;

public class SnipptLengthCheckTests extends GradleUnitTestCase {
    public void testNoSnippets() {
        SnippetLengthCheck.checkFile(failOnError(), 10, "There a no snippets");
    }

    public void testEmptySnippet() {
        SnippetLengthCheck.checkFile(failOnError(), 10, "// tag::foo", "// end::foo");
    }

    public void testSnippetWithSmallText() {
        SnippetLengthCheck.checkFile(failOnError(), 10, "// tag::foo", "some words", "// end::foo");
    }

    public void testSnippetWithLeadingSpaces() {
        SnippetLengthCheck.checkFile(failOnError(), 10, "  // tag::foo", "  some words", "  // end::foo");
    }

    public void testSnippetWithEmptyLine() {
        SnippetLengthCheck.checkFile(failOnError(), 10, "  // tag::foo", "", "  some words", "  // end::foo");
    }

    public void testSnippetBrokenLeadingSpaces() {
        List<String> collection = new ArrayList<>();
        SnippetLengthCheck.checkFile(collect(collection), 10, "  // tag::foo", "some words", "  // end::foo");
        assertThat(collection, equalTo(singletonList("2: snippet line should start with [  ]")));
    }

    public void testSnippetTooLong() {
        List<String> collection = new ArrayList<>();
        SnippetLengthCheck.checkFile(collect(collection), 10, "  // tag::foo", "  too long words", "  // end::foo");
        assertThat(collection, equalTo(singletonList("2: snippet line should be no more than [10] characters but was [14]")));
    }

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
