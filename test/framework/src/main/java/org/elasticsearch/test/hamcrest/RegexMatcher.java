/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.hamcrest;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.regex.Pattern;

/**
 * Matcher that supports regular expression and allows to provide optional flags
 */
public class RegexMatcher extends TypeSafeMatcher<String> {

    private final String regex;
    private final Pattern pattern;

    public RegexMatcher(String regex) {
        this.regex = regex;
        this.pattern = Pattern.compile(regex);
    }

    public RegexMatcher(String regex, int flag) {
        this.regex = regex;
        this.pattern = Pattern.compile(regex, flag);
    }

    @Override
    protected boolean matchesSafely(String item) {
        return pattern.matcher(item).find();
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(regex);
    }

    public static RegexMatcher matches(String regex) {
        return new RegexMatcher(regex);
    }

    public static RegexMatcher matches(String regex, int flag) {
        return new RegexMatcher(regex, flag);
    }
}
