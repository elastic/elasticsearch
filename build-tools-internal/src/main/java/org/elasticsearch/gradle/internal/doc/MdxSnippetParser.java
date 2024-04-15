/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MdxSnippetParser extends SnippetParser {

    public static final Pattern SNIPPET_PATTERN = Pattern.compile("```(.*)");

    public static final Pattern TEST_RESPONSE_PATTERN = Pattern.compile("\\{\\/\\*\s*TESTRESPONSE(\\[(.*)\\])?\s\\*\\/\\}");
    public static final Pattern TEST_PATTERN = Pattern.compile("\\{\\/\\*\s*TEST(\\[(.*)\\])?\s\\*\\/\\}");

    public MdxSnippetParser(Map<String, String> defaultSubstitutions) {
        super(defaultSubstitutions);
    }

    @Override
    protected void parseLine(List<Snippet> snippets, File rootDir, File docFile, int lineNumber, String line) {
        Matcher snippetStartMatcher = SNIPPET_PATTERN.matcher(line);
        if (snippetStartMatcher.matches()) {
            if (snippetBuilder == null) {
                Path path = rootDir.toPath().relativize(docFile.toPath());
                if (snippetStartMatcher.groupCount() == 1) {
                    String language = snippetStartMatcher.group(1);
                    snippetBuilder = new SnippetBuilder().withPath(path)
                        .withLineNumber(lineNumber + 1)
                        .withName(null)
                        .withSubstitutions(defaultSubstitutions)
                        .withLanguage(language);
                }
            } else {
                snippetBuilder.withEnd(lineNumber + 1);
            }
            return;
        }
        if (testHandled(docFile.getName(), lineNumber, line, snippetBuilder)) {
            return;
        }
        if (testResponseHandled(docFile.getName(), lineNumber, line, snippetBuilder)) {
            return;
        }

        if (snippetBuilder == null) {
            // Outside
            return;
        }
        if (snippetBuilder.notFinished()) {
            // Inside
            // We don't need the annotations
            line = line.replaceAll("<\\d+>", "");
            // nor bookmarks
            line = line.replaceAll("\\[\\^\\d+\\]", "");
            // Nor any trailing spaces
            line = line.replaceAll("\s+$", "");
            snippetBuilder.withContent(line, true);

            return;
        }
        snippets.add(snippetBuilder.build());
        snippetBuilder = null;
    }

    protected Pattern testResponsePattern() {
        return TEST_RESPONSE_PATTERN;
    }

    @NotNull
    protected Pattern testPattern() {
        return TEST_PATTERN;
    }
}
