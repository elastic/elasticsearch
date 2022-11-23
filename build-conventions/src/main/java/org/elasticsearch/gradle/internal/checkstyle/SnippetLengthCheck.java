/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.checkstyle;

import com.puppycrawl.tools.checkstyle.api.AbstractFileSetCheck;
import com.puppycrawl.tools.checkstyle.api.CheckstyleException;
import com.puppycrawl.tools.checkstyle.api.FileText;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Checks the snippets included in the docs aren't too wide to fit on
 * the page.
 * <p>
 * Regions contained in the special <code>noformat</code> tag are exempt from the length
 * check. This region is also exempt from automatic formatting.
 */
public class SnippetLengthCheck extends AbstractFileSetCheck {
    private static final Pattern START = Pattern.compile("^( *)//\\s*tag::(?!noformat)(.+?)\\s*$", Pattern.MULTILINE);
    private int max;

    /**
     * The maximum width that a snippet may have.
     */
    public void setMax(int max) {
        this.max = max;
    }

    @Override
    protected void processFiltered(File file, FileText fileText) throws CheckstyleException {
        checkFile((line, message) -> log(line, message), max, fileText.toLinesArray());
    }

    static void checkFile(BiConsumer<Integer, String> log, int max, String... lineArray) {
        LineItr lines = new LineItr(Arrays.asList(lineArray).iterator());
        while (lines.hasNext()) {
            Matcher m = START.matcher(lines.next());
            if (m.matches()) {
                checkSnippet(log, max, lines, m.group(1), m.group(2));
            }
        }
    }

    private static void checkSnippet(BiConsumer<Integer, String> log, int max, LineItr lines, String leadingSpaces, String name) {
        Pattern end = Pattern.compile("^ *//\\s*end::" + name + "\\s*$", Pattern.MULTILINE);
        while (lines.hasNext()) {
            String line = lines.next();
            if (end.matcher(line).matches()) {
                return;
            }
            if (line.isEmpty()) {
                continue;
            }
            if (false == line.startsWith(leadingSpaces)) {
                log.accept(lines.lastLineNumber, "snippet line should start with [" + leadingSpaces + "]");
                continue;
            }
            int width = line.length() - leadingSpaces.length();
            if (width > max) {
                log.accept(lines.lastLineNumber, "snippet line should be no more than [" + max + "] characters but was [" + width + "]");
            }
        }
    }

    private static class LineItr implements Iterator<String> {
        private final Iterator<String> delegate;
        private int lastLineNumber;

        LineItr(Iterator<String> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public String next() {
            lastLineNumber++;
            return delegate.next();
        }
    }
}
