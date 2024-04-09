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

public class Snippet {
    static final int NOT_FINISHED = -1;

    /**
     * Path to the file containing this snippet. Relative to docs.dir of the
     * SnippetsTask that created it.
     */
    Path path;
    int start;
    int end = NOT_FINISHED;
    public String contents;

    Boolean console = null;
    boolean test = false;
    boolean testResponse = false;
    boolean testSetup = false;
    boolean testTearDown = false;
    String skip = null;
    boolean continued = false;
    String language = null;
    String catchPart = null;
    String setup = null;
    String teardown = null;
    boolean curl;
    List<String> warnings = new ArrayList();
    boolean skipShardsFailures = false;
    String name;

    public Snippet(Path path, int start, String name) {
        this.path = path;
        this.start = start;
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

}
