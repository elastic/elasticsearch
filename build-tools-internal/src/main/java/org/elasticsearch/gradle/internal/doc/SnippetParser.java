/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc;

import org.gradle.api.InvalidUserDataException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public abstract class SnippetParser {
    protected static final String SCHAR = "(?:\\\\\\/|[^\\/])";
    protected static final String NON_JSON = "(non_json)";
    protected static final String SKIP_REGEX = "skip:([^\\]]+)";
    protected static final String SUBSTITUTION = "s\\/(" + SCHAR + "+)\\/(" + SCHAR + "*)\\/";

    private static final String CATCH = "catch:\\s*((?:\\/[^\\/]+\\/)|[^ \\]]+)";
    private static final String SETUP = "setup:([^ \\]]+)";
    private static final String TEARDOWN = "teardown:([^ \\]]+)";
    private static final String WARNING = "warning:(.+)";
    private static final String TEST_SYNTAX = "(?:"
        + CATCH
        + "|"
        + SUBSTITUTION
        + "|"
        + SKIP_REGEX
        + "|(continued)|"
        + SETUP
        + "|"
        + TEARDOWN
        + "|"
        + WARNING
        + "|(skip_shard_failures)) ?";

    protected final Map<String, String> defaultSubstitutions;

    protected SnippetBuilder snippetBuilder = null;

    public SnippetParser(Map<String, String> defaultSubstitutions) {
        this.defaultSubstitutions = defaultSubstitutions;
    }

    public List<Snippet> parseDoc(File rootDir, File docFile) {
        List<Snippet> snippets = new ArrayList<>();
        try (Stream<String> lines = Files.lines(docFile.toPath(), StandardCharsets.UTF_8)) {
            List<String> linesList = lines.toList();
            for (int lineNumber = 0; lineNumber < linesList.size(); lineNumber++) {
                String line = linesList.get(lineNumber);
                parseLine(snippets, rootDir, docFile, lineNumber, line);
            }
            fileParsingFinished(snippets);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return snippets;
    }

    private void fileParsingFinished(List<Snippet> snippets) {
        if (snippetBuilder != null) {
            snippets.add(snippetBuilder.build());
            snippetBuilder = null;
        }
    }

    protected abstract void parseLine(List<Snippet> snippets, File rootDir, File docFile, int lineNumber, String line);

    boolean testResponseHandled(String name, int lineNumber, String line, SnippetBuilder snippetBuilder) {
        Matcher matcher = testResponsePattern().matcher(line);
        if (matcher.matches()) {
            if (snippetBuilder == null) {
                throw new InvalidUserDataException(name + ":" + lineNumber + ": TESTRESPONSE not paired with a snippet at ");
            }
            snippetBuilder.withTestResponse(true);
            if (matcher.group(2) != null) {
                String loc = name + ":" + lineNumber;
                ParsingUtils.parse(
                    loc,
                    matcher.group(2),
                    "(?:" + SUBSTITUTION + "|" + NON_JSON + "|" + SKIP_REGEX + ") ?",
                    (Matcher m, Boolean last) -> {
                        if (m.group(1) != null) {
                            // TESTRESPONSE[s/adsf/jkl/]
                            snippetBuilder.withSubstitution(m.group(1), m.group(2));
                        } else if (m.group(3) != null) {
                            // TESTRESPONSE[non_json]
                            snippetBuilder.withSubstitution("^", "/");
                            snippetBuilder.withSubstitution("\n$", "\\\\s*/");
                            snippetBuilder.withSubstitution("( +)", "$1\\\\s+");
                            snippetBuilder.withSubstitution("\n", "\\\\s*\n ");
                        } else if (m.group(4) != null) {
                            // TESTRESPONSE[skip:reason]
                            snippetBuilder.withSkip(m.group(4));
                        }
                    }
                );
            }
            return true;
        }
        return false;
    }

    protected boolean testHandled(String name, int lineNumber, String line, SnippetBuilder snippetBuilder) {
        Matcher matcher = testPattern().matcher(line);
        if (matcher.matches()) {
            if (snippetBuilder == null) {
                throw new InvalidUserDataException(name + ":" + lineNumber + ": TEST not paired with a snippet at ");
            }
            snippetBuilder.withTest(true);
            if (matcher.group(2) != null) {
                String loc = name + ":" + lineNumber;
                ParsingUtils.parse(loc, matcher.group(2), TEST_SYNTAX, (Matcher m, Boolean last) -> {
                    if (m.group(1) != null) {
                        snippetBuilder.withCatchPart(m.group(1));
                        return;
                    }
                    if (m.group(2) != null) {
                        snippetBuilder.withSubstitution(m.group(2), m.group(3));
                        return;
                    }
                    if (m.group(4) != null) {
                        snippetBuilder.withSkip(m.group(4));
                        return;
                    }
                    if (m.group(5) != null) {
                        snippetBuilder.withContinued(true);
                        return;
                    }
                    if (m.group(6) != null) {
                        snippetBuilder.withSetup(m.group(6));
                        return;
                    }
                    if (m.group(7) != null) {
                        snippetBuilder.withTeardown(m.group(7));
                        return;
                    }
                    if (m.group(8) != null) {
                        snippetBuilder.withWarning(m.group(8));
                        return;
                    }
                    if (m.group(9) != null) {
                        snippetBuilder.withSkipShardsFailures(true);
                        return;
                    }
                    throw new InvalidUserDataException("Invalid test marker: " + line);
                });
            }
            return true;
        }
        return false;
    }

    protected abstract Pattern testPattern();

    protected abstract Pattern testResponsePattern();

}
