/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

public class NeverMatcher<T> extends BaseMatcher<T> {
    @SuppressWarnings("unchecked")
    public static <T> Matcher<T> never() {
        return (Matcher<T>) INSTANCE;
    }

    private static final Matcher<?> INSTANCE = new NeverMatcher<>();

    private NeverMatcher() {/* singleton */}

    @Override
    public boolean matches(Object actual) {
        return false;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("never matches");
    }
}
