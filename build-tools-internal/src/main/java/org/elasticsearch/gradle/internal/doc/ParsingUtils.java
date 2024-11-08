/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.doc;

import org.gradle.api.InvalidUserDataException;

import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParsingUtils {

    static void extraContent(String message, String content, int offset, String pattern) {
        StringBuilder cutOut = new StringBuilder();
        cutOut.append(content.substring(offset - 6, offset));
        cutOut.append('*');
        cutOut.append(content.substring(offset, Math.min(offset + 5, content.length())));
        String cutOutNoNl = cutOut.toString().replace("\n", "\\n");
        throw new InvalidUserDataException("Extra content " + message + " ('" + cutOutNoNl + "') matching [" + pattern + "]: " + content);
    }

    /**
     * Repeatedly match the pattern to the string, calling the closure with the
     * matchers each time there is a match. If there are characters that don't
     * match then blow up. If the closure takes two parameters then the second
     * one is "is this the last match?".
     */
    static void parse(String content, String pattern, BiConsumer<Matcher, Boolean> testHandler) {
        if (content == null) {
            return; // Silly null, only real stuff gets to match!
        }
        Matcher m = Pattern.compile(pattern).matcher(content);
        int offset = 0;
        while (m.find()) {
            if (m.start() != offset) {
                extraContent("between [$offset] and [${m.start()}]", content, offset, pattern);
            }
            offset = m.end();
            testHandler.accept(m, offset == content.length());
        }
        if (offset == 0) {
            throw new InvalidUserDataException("Didn't match " + pattern + ": " + content);
        }
        if (offset != content.length()) {
            extraContent("after [" + offset + "]", content, offset, pattern);
        }
    }

}
