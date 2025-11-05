/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common.matchers;

import org.apache.lucene.util.BytesRef;
import org.hamcrest.TypeSafeMatcher;

/**
 * Test matcher for BytesRef that expects BytesRefs, but describes the errors as strings, for better readability.
 */
public class StringBytesRefMatcher extends TypeSafeMatcher<BytesRef> {
    private final String string;
    private final BytesRef bytesRef;

    public StringBytesRefMatcher(String string) {
        this.string = string;
        this.bytesRef = new BytesRef(string);
    }

    @Override
    protected boolean matchesSafely(BytesRef item) {
        return item.equals(bytesRef);
    }

    @Override
    public void describeMismatchSafely(BytesRef item, org.hamcrest.Description description) {
        description.appendText("was ").appendValue(item.utf8ToString());
    }

    @Override
    public void describeTo(org.hamcrest.Description description) {
        description.appendText(string);
    }
}
