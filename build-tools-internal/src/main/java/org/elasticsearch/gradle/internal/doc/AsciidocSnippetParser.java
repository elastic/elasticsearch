/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc;

import org.gradle.api.InvalidUserDataException;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AsciidocSnippetParser extends SnippetParser {
    public static final Pattern SNIPPET_PATTERN = Pattern.compile("-{4,}\\s*");

    public static final Pattern TEST_RESPONSE_PATTERN = Pattern.compile("\\/\\/\s*TESTRESPONSE(\\[(.+)\\])?\s*");

    public AsciidocSnippetParser(Map<String, String> defaultSubstitutions) {
        super(defaultSubstitutions);
    }

    @Override
    protected Pattern testResponsePattern() {
        return TEST_RESPONSE_PATTERN;
    }

    @NotNull
    protected Pattern testPattern() {
        return Pattern.compile("\\/\\/\s*TEST(\\[(.+)\\])?\s*");
    }

    private int lastLanguageLine = 0;
    private String currentName = null;
    private String lastLanguage = null;

    protected void parseLine(List<Snippet> snippets, File rootDir, File docFile, int lineNumber, String line) {
        if (SNIPPET_PATTERN.matcher(line).matches()) {
            if (snippetBuilder == null) {
                Path path = rootDir.toPath().relativize(docFile.toPath());
                snippetBuilder = new SnippetBuilder().withPath(path)
                    .withLineNumber(lineNumber + 1)
                    .withName(currentName)
                    .withSubstitutions(defaultSubstitutions);
                if (lastLanguageLine == lineNumber - 1) {
                    snippetBuilder.withLanguage(lastLanguage);
                }
                currentName = null;
            } else {
                snippetBuilder.withEnd(lineNumber + 1);
            }
            return;
        }

        Source source = matchSource(line);
        if (source.matches) {
            lastLanguage = source.language;
            lastLanguageLine = lineNumber;
            currentName = source.name;
            return;
        }
        if (consoleHandled(docFile.getName(), lineNumber, line, snippetBuilder)) {
            return;
        }
        if (testHandled(docFile.getName(), lineNumber, line, snippetBuilder)) {
            return;
        }
        if (testResponseHandled(docFile.getName(), lineNumber, line, snippetBuilder)) {
            return;
        }
        if (line.matches("\\/\\/\s*TESTSETUP\s*")) {
            snippetBuilder.withTestSetup(true);
            return;
        }
        if (line.matches("\\/\\/\s*TEARDOWN\s*")) {
            snippetBuilder.withTestTearDown(true);
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
            // Nor any trailing spaces
            line = line.replaceAll("\s+$", "");
            snippetBuilder.withContent(line, true);
            return;
        }
        // Allow line continuations for console snippets within lists
        if (snippetBuilder != null && line.trim().equals("+")) {
            return;
        }
        snippets.add(snippetBuilder.build());
        snippetBuilder = null;
    }

    private boolean consoleHandled(String fileName, int lineNumber, String line, SnippetBuilder snippet) {
        if (line.matches("\\/\\/\s*CONSOLE\s*")) {
            if (snippetBuilder == null) {
                throw new InvalidUserDataException(fileName + ":" + lineNumber + ": CONSOLE not paired with a snippet");
            }
            if (snippetBuilder.consoleDefined()) {
                throw new InvalidUserDataException(fileName + ":" + lineNumber + ": Can't be both CONSOLE and NOTCONSOLE");
            }
            snippetBuilder.withConsole(Boolean.TRUE);
            return true;
        } else if (line.matches("\\/\\/\s*NOTCONSOLE\s*")) {
            if (snippet == null) {
                throw new InvalidUserDataException(fileName + ":" + lineNumber + ": NOTCONSOLE not paired with a snippet");
            }
            if (snippetBuilder.consoleDefined()) {
                throw new InvalidUserDataException(fileName + ":" + lineNumber + ": Can't be both CONSOLE and NOTCONSOLE");
            }
            snippet.withConsole(Boolean.FALSE);
            return true;
        }
        return false;
    }

    static Source matchSource(String line) {
        Pattern pattern = Pattern.compile("\\[\"?source\"?(?:\\.[^,]+)?,\\s*\"?([-\\w]+)\"?(,((?!id=).)*(id=\"?([-\\w]+)\"?)?(.*))?].*");
        Matcher matcher = pattern.matcher(line);
        if (matcher.matches()) {
            return new Source(true, matcher.group(1), matcher.group(5));
        }
        return new Source(false, null, null);
    }
}
