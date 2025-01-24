/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.doc;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;

import org.apache.commons.collections.map.MultiValueMap;
import org.gradle.api.InvalidUserDataException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class SnippetBuilder {
    static final int NOT_FINISHED = -1;

    private Path path;
    private int lineNumber;
    private String name;
    private String language;
    private int end = NOT_FINISHED;
    private boolean testSetup;
    private boolean testTeardown;
    // some tests rely on ugly regex substitutions using the same key multiple times
    private MultiValueMap substitutions = MultiValueMap.decorate(new LinkedHashMap<String, String>());
    private String catchPart;
    private boolean test;
    private String skip;
    private boolean continued;
    private String setup;
    private String teardown;
    private List<String> warnings = new ArrayList<>();
    private boolean skipShardsFailures;
    private boolean testResponse;
    private boolean curl;

    private StringBuilder contentBuilder = new StringBuilder();
    private Boolean console = null;

    public SnippetBuilder withPath(Path path) {
        this.path = path;
        return this;
    }

    public SnippetBuilder withLineNumber(int lineNumber) {
        this.lineNumber = lineNumber;
        return this;
    }

    public SnippetBuilder withName(String currentName) {
        this.name = currentName;
        return this;
    }

    public SnippetBuilder withLanguage(String language) {
        this.language = language;
        return this;
    }

    public SnippetBuilder withEnd(int end) {
        this.end = end;
        return this;
    }

    public SnippetBuilder withTestSetup(boolean testSetup) {
        this.testSetup = testSetup;
        return this;
    }

    public SnippetBuilder withTestTearDown(boolean testTeardown) {
        this.testTeardown = testTeardown;
        return this;
    }

    public boolean notFinished() {
        return end == NOT_FINISHED;
    }

    public SnippetBuilder withSubstitutions(Map<String, String> substitutions) {
        this.substitutions.putAll(substitutions);
        return this;
    }

    public SnippetBuilder withSubstitution(String key, String value) {
        this.substitutions.put(key, value);
        return this;
    }

    public SnippetBuilder withTest(boolean test) {
        this.test = test;
        return this;
    }

    public SnippetBuilder withCatchPart(String catchPart) {
        this.catchPart = catchPart;
        return this;
    }

    public SnippetBuilder withSkip(String skip) {
        this.skip = skip;
        return this;
    }

    public SnippetBuilder withContinued(boolean continued) {
        this.continued = continued;
        return this;
    }

    public SnippetBuilder withSetup(String setup) {
        this.setup = setup;
        return this;
    }

    public SnippetBuilder withTeardown(String teardown) {
        this.teardown = teardown;
        return this;
    }

    public SnippetBuilder withWarning(String warning) {
        this.warnings.add(warning);
        return this;
    }

    public SnippetBuilder withSkipShardsFailures(boolean skipShardsFailures) {
        this.skipShardsFailures = skipShardsFailures;
        return this;
    }

    public SnippetBuilder withTestResponse(boolean testResponse) {
        this.testResponse = testResponse;
        return this;
    }

    public SnippetBuilder withContent(String content) {
        return withContent(content, false);
    }

    public SnippetBuilder withContent(String content, boolean newLine) {
        contentBuilder.append(content);
        if (newLine) {
            contentBuilder.append("\n");
        }
        return this;
    }

    private String escapeSubstitutions(String contents) {
        Set<Map.Entry<String, List<String>>> set = substitutions.entrySet();
        for (Map.Entry<String, List<String>> substitution : set) {
            String pattern = substitution.getKey();
            for (String subst : substitution.getValue()) {
                /*
                 * $body is really common, but it looks like a
                 * backreference, so we just escape it here to make the
                 * tests cleaner.
                 */
                subst = subst.replace("$body", "\\$body");
                subst = subst.replace("$_path", "\\$_path");
                subst = subst.replace("\\n", "\n");
                contents = contents.replaceAll(pattern, subst);
            }
        }
        return contents;
    }

    public Snippet build() {
        String content = contentBuilder.toString();
        validate(content);
        String finalContent = escapeSubstitutions(content);
        return new Snippet(
            path,
            lineNumber,
            end,
            finalContent,
            console,
            test,
            testResponse,
            testSetup,
            testTeardown,
            skip,
            continued,
            language,
            catchPart,
            setup,
            teardown,
            curl,
            warnings,
            skipShardsFailures,
            name
        );
    }

    public void validate(String content) {
        if (language == null) {
            throw new InvalidUserDataException(
                name
                    + ": "
                    + "Snippet missing a language. This is required by "
                    + "Elasticsearch's doc testing infrastructure so we "
                    + "be sure we don't accidentally forget to test a "
                    + "snippet."
            );
        }
        assertValidCurlInput(content);
        assertValidJsonInput(content);

    }

    private void assertValidCurlInput(String content) {
        // Try to detect snippets that contain `curl`
        if ("sh".equals(language) || "shell".equals(language)) {
            curl = content.contains("curl");
            if (console == Boolean.FALSE && curl == false) {
                throw new InvalidUserDataException(name + ": " + "No need for NOTCONSOLE if snippet doesn't " + "contain `curl`.");
            }
        }
    }

    private void assertValidJsonInput(String content) {
        if (testResponse && ("js" == language || "console-result" == language) && null == skip) {
            String quoted = content
                // quote values starting with $
                .replaceAll("([:,])\\s*(\\$[^ ,\\n}]+)", "$1 \"$2\"")
                // quote fields starting with $
                .replaceAll("(\\$[^ ,\\n}]+)\\s*:", "\"$1\":");

            JsonFactory jf = new JsonFactory();
            jf.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
            JsonParser jsonParser;

            try {
                jsonParser = jf.createParser(quoted);
                while (jsonParser.isClosed() == false) {
                    jsonParser.nextToken();
                }
            } catch (JsonParseException e) {
                throw new InvalidUserDataException(
                    "Invalid json in "
                        + name
                        + ". The error is:\n"
                        + e.getMessage()
                        + ".\n"
                        + "After substitutions and munging, the json looks like:\n"
                        + quoted,
                    e
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public SnippetBuilder withConsole(Boolean console) {
        this.console = console;
        return this;
    }

    public boolean consoleDefined() {
        return console != null;

    }
}
