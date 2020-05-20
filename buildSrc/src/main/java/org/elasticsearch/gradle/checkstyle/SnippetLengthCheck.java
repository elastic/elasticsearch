/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.checkstyle;

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
 */
public class SnippetLengthCheck extends AbstractFileSetCheck {
    private static final Pattern START = Pattern.compile("^( *)//\\s*tag::(.+?)\\s*$", Pattern.MULTILINE);
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
