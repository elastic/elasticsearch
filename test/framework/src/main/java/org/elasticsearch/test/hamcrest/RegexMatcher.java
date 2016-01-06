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
