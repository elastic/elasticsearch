/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Equality matcher for {@link ByteSizeValue} that has a nice description of failures.
 */
public class ByteSizeEqualsMatcher extends TypeSafeMatcher<ByteSizeValue> {
    public static ByteSizeEqualsMatcher byteSizeEquals(ByteSizeValue expected) {
        return new ByteSizeEqualsMatcher(expected);
    }

    private final ByteSizeValue expected;

    private ByteSizeEqualsMatcher(ByteSizeValue expected) {
        this.expected = expected;
    }

    @Override
    protected boolean matchesSafely(ByteSizeValue byteSizeValue) {
        return expected.equals(byteSizeValue);
    }

    @Override
    public void describeTo(Description description) {
        description.appendValue(expected.toString()).appendText(" (").appendValue(expected.getBytes()).appendText(" bytes)");
    }

    @Override
    protected void describeMismatchSafely(ByteSizeValue item, Description mismatchDescription) {
        mismatchDescription.appendValue(item.toString()).appendText(" (").appendValue(item.getBytes()).appendText(" bytes)");
    }
}
