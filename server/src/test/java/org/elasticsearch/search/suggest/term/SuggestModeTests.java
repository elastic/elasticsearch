/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
