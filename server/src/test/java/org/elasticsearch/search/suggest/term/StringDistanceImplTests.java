/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.term;

import org.elasticsearch.common.io.stream.AbstractWriteableEnumTestCase;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder.StringDistanceImpl;

import java.io.IOException;

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
        assertThat(StringDistanceImpl.LEVENSHTEIN.ordinal(), equalTo(2));
        assertThat(StringDistanceImpl.JARO_WINKLER.ordinal(), equalTo(3));
        assertThat(StringDistanceImpl.NGRAM.ordinal(), equalTo(4));
    }

    @Override
    public void testFromString() {
        assertThat(StringDistanceImpl.resolve("internal"), equalTo(StringDistanceImpl.INTERNAL));
        assertThat(StringDistanceImpl.resolve("damerau_levenshtein"), equalTo(StringDistanceImpl.DAMERAU_LEVENSHTEIN));
        assertThat(StringDistanceImpl.resolve("levenshtein"), equalTo(StringDistanceImpl.LEVENSHTEIN));
        assertThat(StringDistanceImpl.resolve("jaro_winkler"), equalTo(StringDistanceImpl.JARO_WINKLER));
        assertThat(StringDistanceImpl.resolve("ngram"), equalTo(StringDistanceImpl.NGRAM));

        final String doesntExist = "doesnt_exist";
        expectThrows(IllegalArgumentException.class, () -> StringDistanceImpl.resolve(doesntExist));

        NullPointerException e = expectThrows(NullPointerException.class, () -> StringDistanceImpl.resolve(null));
        assertThat(e.getMessage(), equalTo("Input string is null"));
    }

    @Override
    public void testWriteTo() throws IOException {
        assertWriteToStream(StringDistanceImpl.INTERNAL, 0);
        assertWriteToStream(StringDistanceImpl.DAMERAU_LEVENSHTEIN, 1);
        assertWriteToStream(StringDistanceImpl.LEVENSHTEIN, 2);
        assertWriteToStream(StringDistanceImpl.JARO_WINKLER, 3);
        assertWriteToStream(StringDistanceImpl.NGRAM, 4);
    }

    @Override
    public void testReadFrom() throws IOException {
        assertReadFromStream(0, StringDistanceImpl.INTERNAL);
        assertReadFromStream(1, StringDistanceImpl.DAMERAU_LEVENSHTEIN);
        assertReadFromStream(2, StringDistanceImpl.LEVENSHTEIN);
        assertReadFromStream(3, StringDistanceImpl.JARO_WINKLER);
        assertReadFromStream(4, StringDistanceImpl.NGRAM);
    }
}
