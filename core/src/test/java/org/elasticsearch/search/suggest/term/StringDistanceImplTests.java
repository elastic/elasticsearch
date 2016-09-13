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

import static org.elasticsearch.search.suggest.term.TermSuggestionBuilder.StringDistanceImpl;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test for the {@link StringDistanceImpl} enum.
 */
public class StringDistanceImplTests extends AbstractWriteableEnumTestCase {
    public StringDistanceImplTests() {
        super(StringDistanceImpl::readFromStream);
    }

    @Override
    public void testValidOrdinals() {
        assertThat(StringDistanceImpl.INTERNAL.ordinal(), equalTo(0));
        assertThat(StringDistanceImpl.DAMERAU_LEVENSHTEIN.ordinal(), equalTo(1));
        assertThat(StringDistanceImpl.LEVENSTEIN.ordinal(), equalTo(2));
        assertThat(StringDistanceImpl.JAROWINKLER.ordinal(), equalTo(3));
        assertThat(StringDistanceImpl.NGRAM.ordinal(), equalTo(4));
    }

    @Override
    public void testFromString() {
        assertThat(StringDistanceImpl.resolve("internal"), equalTo(StringDistanceImpl.INTERNAL));
        assertThat(StringDistanceImpl.resolve("damerau_levenshtein"), equalTo(StringDistanceImpl.DAMERAU_LEVENSHTEIN));
        assertThat(StringDistanceImpl.resolve("levenstein"), equalTo(StringDistanceImpl.LEVENSTEIN));
        assertThat(StringDistanceImpl.resolve("jarowinkler"), equalTo(StringDistanceImpl.JAROWINKLER));
        assertThat(StringDistanceImpl.resolve("ngram"), equalTo(StringDistanceImpl.NGRAM));
        final String doesntExist = "doesnt_exist";
        try {
            StringDistanceImpl.resolve(doesntExist);
            fail("StringDistanceImpl should not have an element " + doesntExist);
        } catch (IllegalArgumentException e) {
        }
        try {
            StringDistanceImpl.resolve(null);
            fail("StringDistanceImpl.resolve on a null value should throw an exception.");
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), equalTo("Input string is null"));
        }
    }

    @Override
    public void testWriteTo() throws IOException {
        assertWriteToStream(StringDistanceImpl.INTERNAL, 0);
        assertWriteToStream(StringDistanceImpl.DAMERAU_LEVENSHTEIN, 1);
        assertWriteToStream(StringDistanceImpl.LEVENSTEIN, 2);
        assertWriteToStream(StringDistanceImpl.JAROWINKLER, 3);
        assertWriteToStream(StringDistanceImpl.NGRAM, 4);
    }

    @Override
    public void testReadFrom() throws IOException {
        assertReadFromStream(0, StringDistanceImpl.INTERNAL);
        assertReadFromStream(1, StringDistanceImpl.DAMERAU_LEVENSHTEIN);
        assertReadFromStream(2, StringDistanceImpl.LEVENSTEIN);
        assertReadFromStream(3, StringDistanceImpl.JAROWINKLER);
        assertReadFromStream(4, StringDistanceImpl.NGRAM);
    }

}
