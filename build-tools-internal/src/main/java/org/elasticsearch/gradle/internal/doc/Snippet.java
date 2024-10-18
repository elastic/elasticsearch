/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.doc;

import java.nio.file.Path;
import java.util.List;

public record Snippet(
    Path path,
    int start,
    int end,
    String contents,
    Boolean console,
    boolean test,
    boolean testResponse,
    boolean testSetup,
    boolean testTearDown,
    String skip,
    boolean continued,
    String language,
    String catchPart,
    String setup,
    String teardown,
    boolean curl,
    List<String> warnings,
    boolean skipShardsFailures,
    String name
) {

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
