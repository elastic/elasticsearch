/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;

import org.gradle.api.InvalidUserDataException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Snippet {
    static final int NOT_FINISHED = -1;

    /**
     * Path to the file containing this snippet. Relative to docs.dir of the
     * SnippetsTask that created it.
     */
    private Path path;
    private int start;
    private int end = NOT_FINISHED;
    private String contents;

    private Boolean console = null;
    private boolean test = false;
    private boolean testResponse = false;
    private boolean testSetup = false;
    private boolean testTearDown = false;
    private String skip = null;
    private boolean continued = false;
    private String language = null;
    private String catchPart = null;
    private String setup = null;
    private String teardown = null;
    private boolean curl;
    private List<String> warnings = new ArrayList();
    boolean skipShardsFailures = false;
    private String name;

    public Snippet(Path path, int start, String name) {
        this.path = path;
        this.start = start;
        this.name = name;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public String getContents() {
        return contents;
    }

    public void setContents(String contents) {
        this.contents = contents;
    }

    public Boolean getConsole() {
        return console;
    }

    public void setConsole(Boolean console) {
        this.console = console;
    }

    public boolean isTest() {
        return test;
    }

    public void setTest(boolean test) {
        this.test = test;
    }

    public boolean isTestResponse() {
        return testResponse;
    }

    public void setTestResponse(boolean testResponse) {
        this.testResponse = testResponse;
    }

    public boolean isTestSetup() {
        return testSetup;
    }

    public void setTestSetup(boolean testSetup) {
        this.testSetup = testSetup;
    }

    public boolean isTestTearDown() {
        return testTearDown;
    }

    public void setTestTearDown(boolean testTearDown) {
        this.testTearDown = testTearDown;
    }

    public String getSkip() {
        return skip;
    }

    public void setSkip(String skip) {
        this.skip = skip;
    }

    public boolean isContinued() {
        return continued;
    }

    public void setContinued(boolean continued) {
        this.continued = continued;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getCatchPart() {
        return catchPart;
    }

    public void setCatchPart(String catchPart) {
        this.catchPart = catchPart;
    }

    public String getSetup() {
        return setup;
    }

    public void setSetup(String setup) {
        this.setup = setup;
    }

    public String getTeardown() {
        return teardown;
    }

    public void setTeardown(String teardown) {
        this.teardown = teardown;
    }

    public boolean isCurl() {
        return curl;
    }

    public void setCurl(boolean curl) {
        this.curl = curl;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public void setWarnings(List<String> warnings) {
        this.warnings = warnings;
    }

    public boolean isSkipShardsFailures() {
        return skipShardsFailures;
    }

    public void setSkipShardsFailures(boolean skipShardsFailures) {
        this.skipShardsFailures = skipShardsFailures;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void validate() {
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
        assertValidCurlInput();
        assertValidJsonInput();
    }

    String getLocation() {
        return path + "[" + start + ":" + end + "]";
    }

    private void assertValidCurlInput() {
        // Try to detect snippets that contain `curl`
        if ("sh".equals(language) || "shell".equals(language)) {
            curl = contents.contains("curl");
            if (console == Boolean.FALSE && curl == false) {
                throw new InvalidUserDataException(name + ": " + "No need for NOTCONSOLE if snippet doesn't " + "contain `curl`.");
            }
        }
    }

    private void assertValidJsonInput() {
        if (testResponse && ("js" == language || "console-result" == language) && null == skip) {
            String quoted = contents
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

    @Override
    public String toString() {
        String result = path + "[" + start + ":" + end + "]";
        if (language != null) {
            result += "(" + language + ")";
        }
        if (console != null) {
            result += console ? "// CONSOLE" : "// NOTCONSOLE";
        }
        if (test) {
            result += "// TEST";
            if (catchPart != null) {
                result += "[catch: " + catchPart + "]";
            }
            if (skip != null) {
                result += "[skip=" + skip + "]";
            }
            if (continued) {
                result += "[continued]";
            }
            if (setup != null) {
                result += "[setup:" + setup + "]";
            }
            if (teardown != null) {
                result += "[teardown:" + teardown + "]";
            }
            for (String warning : warnings) {
                result += "[warning:" + warning + "]";
            }
            if (skipShardsFailures) {
                result += "[skip_shard_failures]";
            }
        }
        if (testResponse) {
            result += "// TESTRESPONSE";
            if (skip != null) {
                result += "[skip=" + skip + "]";
            }
        }
        if (testSetup) {
            result += "// TESTSETUP";
        }
        if (curl) {
            result += "(curl)";
        }
        return result;
    }

    /**
     * Is this snippet a candidate for conversion to `// CONSOLE`?
     */
    boolean isConsoleCandidate() {
        /* Snippets that are responses or already marked as `// CONSOLE` or
         * `// NOTCONSOLE` are not candidates. */
        if (console != null || testResponse) {
            return false;
        }
        /* js snippets almost always should be marked with `// CONSOLE`. js
         * snippets that shouldn't be marked `// CONSOLE`, like examples for
         * js client, should always be marked with `// NOTCONSOLE`.
         *
         * `sh` snippets that contain `curl` almost always should be marked
         * with `// CONSOLE`. In the exceptionally rare cases where they are
         * not communicating with Elasticsearch, like the examples in the ec2
         * and gce discovery plugins, the snippets should be marked
         * `// NOTCONSOLE`. */
        return language.equals("js") || curl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Snippet snippet = (Snippet) o;
        return start == snippet.start
            && end == snippet.end
            && test == snippet.test
            && testResponse == snippet.testResponse
            && testSetup == snippet.testSetup
            && testTearDown == snippet.testTearDown
            && continued == snippet.continued
            && curl == snippet.curl
            && skipShardsFailures == snippet.skipShardsFailures
            && Objects.equals(path, snippet.path)
            && Objects.equals(contents, snippet.contents)
            && Objects.equals(console, snippet.console)
            && Objects.equals(skip, snippet.skip)
            && Objects.equals(language, snippet.language)
            && Objects.equals(catchPart, snippet.catchPart)
            && Objects.equals(setup, snippet.setup)
            && Objects.equals(teardown, snippet.teardown)
            && Objects.equals(warnings, snippet.warnings)
            && Objects.equals(name, snippet.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            path,
            start,
            end,
            contents,
            console,
            test,
            testResponse,
            testSetup,
            testTearDown,
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
}
