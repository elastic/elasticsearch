/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.doc;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MdxSnippetParser extends SnippetParser {

    public static final Pattern SNIPPET_PATTERN = Pattern.compile("```(.*)");

    public static final Pattern TEST_RESPONSE_PATTERN = Pattern.compile("\\{\\/\\*\s*TESTRESPONSE(\\[(.*)\\])?\s\\*\\/\\}");
    public static final Pattern TEST_PATTERN = Pattern.compile("\\{\\/\\*\s*TEST(\\[(.*)\\])?\s\\*\\/\\}");
    public static final String CONSOLE_REGEX = "\\{\\/\\*\s*CONSOLE\s\\*\\/\\}";
    public static final String NOTCONSOLE_REGEX = "\\{\\/\\*\s*NOTCONSOLE\s\\*\\/\\}";
    public static final String TESTSETUP_REGEX = "\\{\\/\\*\s*TESTSETUP\s\\*\\/\\}";
    public static final String TEARDOWN_REGEX = "\\{\\/\\*\s*TEARDOWN\s\\*\\/\\}";

    public MdxSnippetParser(Map<String, String> defaultSubstitutions) {
        super(defaultSubstitutions);
    }

    @Override
    protected void parseLine(List<Snippet> snippets, int lineNumber, String line) {
        Matcher snippetStartMatcher = SNIPPET_PATTERN.matcher(line);
        if (snippetStartMatcher.matches()) {
            if (snippetBuilder == null) {
                if (snippetStartMatcher.groupCount() == 1) {
                    String language = snippetStartMatcher.group(1);
                    snippetBuilder = newSnippetBuilder().withLineNumber(lineNumber + 1)
                        .withName(null)
                        .withSubstitutions(defaultSubstitutions)
                        .withLanguage(language);
                }
            } else {
                snippetBuilder.withEnd(lineNumber + 1);
            }
            return;
        }
        handleCommons(snippets, line);
    }

    @Override
    protected String getTestSetupRegex() {
        return TESTSETUP_REGEX;
    }

    @Override
    protected String getTeardownRegex() {
        return TEARDOWN_REGEX;
    }

    @Override
    protected String getNotconsoleRegex() {
        return NOTCONSOLE_REGEX;
    }

    @Override
    protected String getConsoleRegex() {
        return CONSOLE_REGEX;
    }

    @Override
    protected Pattern testResponsePattern() {
        return TEST_RESPONSE_PATTERN;
    }

    @Override
    protected Pattern testPattern() {
        return TEST_PATTERN;
    }
}
