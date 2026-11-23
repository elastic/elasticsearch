/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions.filters;

import java.io.Reader;
import java.util.Locale;

/**
 * Copy filter that normalizes line endings. Drop-in replacement for the line ending handling of Ant's
 * {@code org.apache.tools.ant.filters.FixCrLfFilter}, which won't be available on the build classpath in Gradle 10.
 *
 * <pre>
 * filter("eol": "crlf", FixCrLfFilter)
 * copySpec.filter(Map.of("eol", "crlf"), FixCrLfFilter.class)
 * </pre>
 *
 * <p>Matches Ant's defaults on non-Windows hosts: a trailing Ctrl-Z (EOF marker) is removed, {@code \n}, {@code \r\n},
 * {@code \r} and {@code \r\r\n} line breaks are converted to the configured line ending, a missing line break at the
 * end of the content is added, and tabs are left as is. Unlike Ant, the result does not depend on the host OS.
 */
public class FixCrLfFilter extends BufferingFilterReader {

    private static final char CTRL_Z = '\u001A';

    private String eol = "\n";

    public FixCrLfFilter(Reader in) {
        super(in);
    }

    /**
     * Sets the line ending to use: {@code lf} / {@code unix}, {@code crlf} / {@code dos} or {@code cr} / {@code mac}.
     */
    public void setEol(String eol) {
        this.eol = switch (eol.toLowerCase(Locale.ROOT)) {
            case "lf", "unix" -> "\n";
            case "crlf", "dos" -> "\r\n";
            case "cr", "mac" -> "\r";
            default -> throw new IllegalArgumentException("Unsupported eol [" + eol + "], expected one of [lf, crlf, cr]");
        };
    }

    @Override
    protected String transform(String content) {
        return normalize(content, eol);
    }

    static String normalize(String content, String eol) {
        int length = content.length();
        // a trailing Ctrl-Z (DOS EOF marker) is removed
        if (length > 0 && content.charAt(length - 1) == CTRL_Z) {
            length--;
        }
        // like Ant, a Ctrl-Z that is still the last character stays last, after the added final line ending
        boolean endsWithCtrlZ = length > 0 && content.charAt(length - 1) == CTRL_Z;
        if (endsWithCtrlZ) {
            length--;
        }
        StringBuilder result = new StringBuilder(length + 16);
        boolean previousWasEol = false;
        int i = 0;
        while (i < length) {
            char c = content.charAt(i);
            int eols = 0;
            if (c == '\n') {
                eols = 1;
                i++;
            } else if (c == '\r') {
                char c1 = i + 1 < length ? content.charAt(i + 1) : 0;
                char c2 = i + 2 < length ? content.charAt(i + 2) : 0;
                if (c1 == '\r' && c2 == '\n') {
                    eols = 1;
                    i += 3;
                } else if (c1 == '\r') {
                    eols = 2;
                    i += 2;
                } else if (c1 == '\n') {
                    eols = 1;
                    i += 2;
                } else {
                    eols = 1;
                    i++;
                }
            } else {
                result.append(c);
                i++;
            }
            if (eols > 0) {
                result.append(eol.repeat(eols));
                previousWasEol = true;
            } else {
                previousWasEol = false;
            }
        }
        if (previousWasEol == false) {
            result.append(eol);
        }
        if (endsWithCtrlZ) {
            result.append(CTRL_Z);
        }
        return result.toString();
    }
}
