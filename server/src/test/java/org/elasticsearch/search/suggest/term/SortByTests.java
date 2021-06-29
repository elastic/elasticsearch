/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.term;

import org.elasticsearch.common.io.stream.AbstractWriteableEnumTestCase;
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
