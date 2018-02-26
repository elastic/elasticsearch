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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.suggest.SortBy;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * Test the {@link SortBy} enum.
 */
public class SortByTests extends AbstractWriteableEnumTestCase {
    public SortByTests() {
        super(SortBy::readFromStream);
    }

    @Override
    public void testValidOrdinals() {
        assertThat(SortBy.SCORE.ordinal(), equalTo(0));
        assertThat(SortBy.FREQUENCY.ordinal(), equalTo(1));
    }

    @Override
    public void testFromString() {
        assertThat(SortBy.resolve("score"), equalTo(SortBy.SCORE));
        assertThat(SortBy.resolve("frequency"), equalTo(SortBy.FREQUENCY));
        final String doesntExist = "doesnt_exist";
        try {
            SortBy.resolve(doesntExist);
            fail("SortBy should not have an element " + doesntExist);
        } catch (IllegalArgumentException e) {
        }
        try {
            SortBy.resolve(null);
            fail("SortBy.resolve on a null value should throw an exception.");
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), equalTo("Input string is null"));
        }
    }

    @Override
    public void testWriteTo() throws IOException {
        assertWriteToStream(SortBy.SCORE, 0);
        assertWriteToStream(SortBy.FREQUENCY, 1);
    }

    @Override
    public void testReadFrom() throws IOException {
        assertReadFromStream(0, SortBy.SCORE);
        assertReadFromStream(1, SortBy.FREQUENCY);
    }
}
