/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.categorization;

import org.apache.lucene.analysis.charfilter.BaseCharFilter;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

/**
 * A character filter that keeps the first line with alpha letters in the input, and discards everything before and after it.
 * Treats both <code>\n</code> and <code>\r\n</code> as line endings.
 *
 * If there is a line ending at the end of the first valid line, this is discarded.
 *
 * A line is considered valid if any {@link Character#isLetter} returns
 * <code>true</code>.
 *
 * It is possible to achieve the same effect with a <code>pattern_replace</code> filter, but since this filter
 * needs to be run on every single message to be categorized it is worth having a more performant specialization.
 */
public class FirstLineWithLettersCharFilter extends BaseCharFilter {

    public static final String NAME = "first_line_with_letters";

    private Reader transformedInput;

    FirstLineWithLettersCharFilter(Reader in) {
        super(in);
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        // Buffer all input on the first call.
        if (transformedInput == null) {
            fill();
        }

        return transformedInput.read(cbuf, off, len);
    }

    @Override
    public int read() throws IOException {
        if (transformedInput == null) {
            fill();
        }

        return transformedInput.read();
    }

    private void fill() throws IOException {
        StringBuilder buffered = new StringBuilder();
        char[] temp = new char[1024];
        for (int cnt = input.read(temp); cnt > 0; cnt = input.read(temp)) {
            buffered.append(temp, 0, cnt);
        }
        transformedInput = new StringReader(process(buffered).toString());
    }

    private CharSequence process(CharSequence input) {

        boolean seenLetter = false;
        int prevNewlineIndex = -1;
        int endIndex = -1;

        for (int index = 0; index < input.length(); ++index) {
            if (input.charAt(index) == '\n') {
                if (seenLetter) {
                    // With Windows line endings chop the \r as well as the \n
                    endIndex = (input.charAt(index - 1) == '\r') ? (index - 1) : index;
                    break;
                }
                prevNewlineIndex = index;
            } else {
                seenLetter = seenLetter || Character.isLetter(input.charAt(index));
            }
        }

        if (seenLetter == false) {
            return "";
        }

        if (endIndex == -1) {
            if (prevNewlineIndex == -1) {
                // This is pretty likely, as most log messages _aren't_ multiline, so worth optimising
                // for even though the return at the end of the method would be functionally identical
                return input;
            }
            endIndex = input.length();
        }

        addOffCorrectMap(0, prevNewlineIndex + 1);
        if (endIndex < input.length()) {
            addOffCorrectMap(endIndex - prevNewlineIndex - 1, input.length() - endIndex + prevNewlineIndex + 1);
        }
        return input.subSequence(prevNewlineIndex + 1, endIndex);
    }
}
