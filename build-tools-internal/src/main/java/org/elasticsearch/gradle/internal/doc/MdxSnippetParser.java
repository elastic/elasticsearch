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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class MdxSnippetParser extends SnippetParser {

    public static final Pattern SNIPPET_PATTERN = Pattern.compile("```(.*)");

    public static final Pattern TEST_RESPONSE_PATTERN = Pattern.compile("\\{\\/\\*\s*TESTRESPONSE(\\[(.*)\\])?\s\\*\\/\\}");
    public static final Pattern TEST_PATTERN = Pattern.compile("\\{\\/\\*\s*TEST(\\[(.*)\\])?\s\\*\\/\\}");
    private final Map<String, String> defaultSubstitutions;

    public MdxSnippetParser(Map<String, String> defaultSubstitutions) {
        this.defaultSubstitutions = defaultSubstitutions;
    }

    @Override
    public List<Snippet> parseDoc(File rootDir, File docFile, List<Map.Entry<String, String>> substitutions) {
        Snippet snippet = null;
        StringBuilder contents = null;
        List<Snippet> snippets = new ArrayList<>();

        try (Stream<String> lines = Files.lines(docFile.toPath(), StandardCharsets.UTF_8)) {
            List<String> linesList = lines.toList();
            for (int lineNumber = 0; lineNumber < linesList.size(); lineNumber++) {
                String line = linesList.get(lineNumber);
                Matcher snippetStartMatcher = SNIPPET_PATTERN.matcher(line);
                if (snippetStartMatcher.matches()) {
                    if (snippet == null) {
                        Path path = rootDir.toPath().relativize(docFile.toPath());
                        if (snippetStartMatcher.groupCount() == 1) {
                            String language = snippetStartMatcher.group(1);
                            snippet = new Snippet(path, lineNumber + 1, null);
                            snippets.add(snippet);
                            snippet.setLanguage(language);
                        }
                    } else {
                        snippet.setEnd(lineNumber + 1);
                    }
                    continue;
                }
                if (testHandled(docFile.getName(), lineNumber, line, snippet, substitutions)) {
                    continue;
                }
                if (testResponseHandled(docFile.getName(), lineNumber, line, snippet, substitutions)) {
                    continue;
                }
                /*
                Source source = matchSource(line);
                if (source.matches) {
                    lastLanguage = source.language;
                    lastLanguageLine = lineNumber;
                    name = source.name;
                    continue;
                }

                if (consoleHandled(docFile.getName(), lineNumber, line, snippet)) {
                    continue;
                }

                if (line.matches("\\/\\/\s*TESTSETUP\s*")) {
                    snippet.testSetup = true;
                    continue;
                }
                if (line.matches("\\/\\/\s*TEARDOWN\s*")) {
                    snippet.testTearDown = true;
                    continue;
                }*/

                if (snippet == null) {
                    // Outside
                    continue;
                }
                if (snippet.getEnd() == Snippet.NOT_FINISHED) {
                    // Inside
                    if (contents == null) {
                        contents = new StringBuilder();
                    }
                    // We don't need the annotations
                    line = line.replaceAll("<\\d+>", "");
                    // nor bookmarks
                    line = line.replaceAll("\\[\\^\\d+\\]", "");

                    // Nor any trailing spaces
                    line = line.replaceAll("\s+$", "");

                    contents.append(line).append("\n");

                    continue;
                }
                // Allow line continuations for console snippets within lists
                /*if (snippet != null && line.trim().equals("+")) {
                    continue;
                }*/
                finalizeSnippet(snippet, contents.toString(), defaultSubstitutions, substitutions);
                substitutions = new ArrayList<>();
                ;
                snippet = null;
                contents = null;
            }
            if (snippet != null) {
                finalizeSnippet(snippet, contents.toString(), defaultSubstitutions, substitutions);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return snippets;

    }

    protected Pattern testResponsePattern() {
        return TEST_RESPONSE_PATTERN;
    }

    @NotNull
    protected Pattern testPattern() {
        return TEST_PATTERN;
    }
}
