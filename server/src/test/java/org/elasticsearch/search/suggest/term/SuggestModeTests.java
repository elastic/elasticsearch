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

package org.elasticsearch.search.suggest.term;

import org.elasticsearch.common.io.stream.AbstractWriteableEnumTestCase;

import java.io.IOException;

import static org.elasticsearch.search.suggest.term.TermSuggestionBuilder.SuggestMode;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test the {@link SuggestMode} enum.
 */
public class SuggestModeTests extends AbstractWriteableEnumTestCase {
    public SuggestModeTests() {
        super(SuggestMode::readFromStream);
    }

    @Override
    public void testValidOrdinals() {
        assertThat(SuggestMode.MISSING.ordinal(), equalTo(0));
        assertThat(SuggestMode.POPULAR.ordinal(), equalTo(1));
        assertThat(SuggestMode.ALWAYS.ordinal(), equalTo(2));
    }

    @Override
    public void testFromString() {
        assertThat(SuggestMode.resolve("missing"), equalTo(SuggestMode.MISSING));
        assertThat(SuggestMode.resolve("popular"), equalTo(SuggestMode.POPULAR));
        assertThat(SuggestMode.resolve("always"), equalTo(SuggestMode.ALWAYS));
        final String doesntExist = "doesnt_exist";
        try {
            SuggestMode.resolve(doesntExist);
            fail("SuggestMode should not have an element " + doesntExist);
        } catch (IllegalArgumentException e) {
        }
        try {
            SuggestMode.resolve(null);
            fail("SuggestMode.resolve on a null value should throw an exception.");
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), equalTo("Input string is null"));
        }
    }

    @Override
    public void testWriteTo() throws IOException {
        assertWriteToStream(SuggestMode.MISSING, 0);
        assertWriteToStream(SuggestMode.POPULAR, 1);
        assertWriteToStream(SuggestMode.ALWAYS, 2);
    }

    @Override
    public void testReadFrom() throws IOException {
        assertReadFromStream(0, SuggestMode.MISSING);
        assertReadFromStream(1, SuggestMode.POPULAR);
        assertReadFromStream(2, SuggestMode.ALWAYS);
    }

}
