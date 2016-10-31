/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class DoSectionTests extends ESTestCase {
    public void testWarningHeaders() throws IOException {
        DoSection section = new DoSection(new XContentLocation(1, 1));

        // No warning headers doesn't throw an exception
        section.checkWarningHeaders(emptyList());

        // Any warning headers fail
        AssertionError e = expectThrows(AssertionError.class, () -> section.checkWarningHeaders(singletonList("test")));
        assertEquals("got unexpected warning headers [\ntest\n]", e.getMessage());
        e = expectThrows(AssertionError.class, () -> section.checkWarningHeaders(Arrays.asList("test", "another", "some more")));
        assertEquals("got unexpected warning headers [\ntest\nanother\nsome more\n]", e.getMessage());

        // But not when we expect them
        section.setExpectedWarningHeaders(singletonList("test"));
        section.checkWarningHeaders(singletonList("test"));
        section.setExpectedWarningHeaders(Arrays.asList("test", "another", "some more"));
        section.checkWarningHeaders(Arrays.asList("test", "another", "some more"));

        // But if you don't get some that you did expect, that is an error
        section.setExpectedWarningHeaders(singletonList("test"));
        e = expectThrows(AssertionError.class, () -> section.checkWarningHeaders(emptyList()));
        assertEquals("didn't get expected warning headers [\ntest\n]", e.getMessage());
        section.setExpectedWarningHeaders(Arrays.asList("test", "another", "some more"));
        e = expectThrows(AssertionError.class, () -> section.checkWarningHeaders(emptyList()));
        assertEquals("didn't get expected warning headers [\ntest\nanother\nsome more\n]", e.getMessage());
        e = expectThrows(AssertionError.class, () -> section.checkWarningHeaders(Arrays.asList("test", "some more")));
        assertEquals("didn't get expected warning headers [\nanother\n]", e.getMessage());

        // It is also an error if you get some warning you want and some you don't want
        section.setExpectedWarningHeaders(Arrays.asList("test", "another", "some more"));
        e = expectThrows(AssertionError.class, () -> section.checkWarningHeaders(Arrays.asList("test", "cat")));
        assertEquals("got unexpected warning headers [\ncat\n] didn't get expected warning headers [\nanother\nsome more\n]",
                e.getMessage());
    }
}
