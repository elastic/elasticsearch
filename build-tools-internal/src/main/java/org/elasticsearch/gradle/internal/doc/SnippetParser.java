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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    abstract List<Snippet> parseDoc(File rootDir, File docFile, List<Map.Entry<String, String>> substitutions);

    static Snippet finalizeSnippet(
        final Snippet snippet,
        String contents,
        Map<String, String> defaultSubstitutions,
        Collection<Map.Entry<String, String>> substitutions
    ) {
        snippet.setContents(contents.toString());
        snippet.validate();
        escapeSubstitutions(snippet, defaultSubstitutions, substitutions);
        return snippet;
    }

    private static void escapeSubstitutions(
        Snippet snippet,
        Map<String, String> defaultSubstitutions,
        Collection<Map.Entry<String, String>> substitutions
    ) {
        BiConsumer<String, String> doSubstitution = (pattern, subst) -> {
            /*
             * $body is really common, but it looks like a
             * backreference, so we just escape it here to make the
             * tests cleaner.
             */
            subst = subst.replace("$body", "\\$body");
            subst = subst.replace("$_path", "\\$_path");
            subst = subst.replace("\\n", "\n");
            snippet.setContents(snippet.getContents().replaceAll(pattern, subst));
        };
        defaultSubstitutions.forEach(doSubstitution);

        if (substitutions != null) {
            substitutions.forEach(e -> doSubstitution.accept(e.getKey(), e.getValue()));
        }
    }

    boolean testResponseHandled(
        String name,
        int lineNumber,
        String line,
        Snippet snippet,
        final List<Map.Entry<String, String>> substitutions
    ) {
        Matcher matcher = testResponsePattern().matcher(line);
        if (matcher.matches()) {
            if (snippet == null) {
                throw new InvalidUserDataException(name + ":" + lineNumber + ": TESTRESPONSE not paired with a snippet at ");
            }
            snippet.setTestResponse(true);
            if (matcher.group(2) != null) {
                String loc = name + ":" + lineNumber;
                ParsingUtils.parse(
                    loc,
                    matcher.group(2),
                    "(?:" + SUBSTITUTION + "|" + NON_JSON + "|" + SKIP_REGEX + ") ?",
                    (Matcher m, Boolean last) -> {
                        if (m.group(1) != null) {
                            // TESTRESPONSE[s/adsf/jkl/]
                            substitutions.add(Map.entry(m.group(1), m.group(2)));
                        } else if (m.group(3) != null) {
                            // TESTRESPONSE[non_json]
                            substitutions.add(Map.entry("^", "/"));
                            substitutions.add(Map.entry("\n$", "\\\\s*/"));
                            substitutions.add(Map.entry("( +)", "$1\\\\s+"));
                            substitutions.add(Map.entry("\n", "\\\\s*\n "));
                        } else if (m.group(4) != null) {
                            // TESTRESPONSE[skip:reason]
                            snippet.setSkip(m.group(4));
                        }
                    }
                );
            }
            return true;
        }
        return false;
    }

    protected boolean testHandled(
        String name,
        int lineNumber,
        String line,
        Snippet snippet,
        List<Map.Entry<String, String>> substitutions
    ) {
        Matcher matcher = testPattern().matcher(line);
        if (matcher.matches()) {
            if (snippet == null) {
                throw new InvalidUserDataException(name + ":" + lineNumber + ": TEST not paired with a snippet at ");
            }
            snippet.setTest(true);
            if (matcher.group(2) != null) {
                String loc = name + ":" + lineNumber;
                ParsingUtils.parse(loc, matcher.group(2), TEST_SYNTAX, (Matcher m, Boolean last) -> {
                    if (m.group(1) != null) {
                        snippet.setCatchPart(m.group(1));
                        return;
                    }
                    if (m.group(2) != null) {
                        substitutions.add(Map.entry(m.group(2), m.group(3)));
                        return;
                    }
                    if (m.group(4) != null) {
                        snippet.setSkip(m.group(4));
                        return;
                    }
                    if (m.group(5) != null) {
                        snippet.setContinued(true);
                        return;
                    }
                    if (m.group(6) != null) {
                        snippet.setSetup(m.group(6));
                        return;
                    }
                    if (m.group(7) != null) {
                        snippet.setTeardown(m.group(7));
                        return;
                    }
                    if (m.group(8) != null) {
                        snippet.getWarnings().add(m.group(8));
                        return;
                    }
                    if (m.group(9) != null) {
                        snippet.setSkipShardsFailures(true);
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
