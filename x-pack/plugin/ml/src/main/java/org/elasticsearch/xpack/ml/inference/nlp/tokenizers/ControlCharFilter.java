/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import com.carrotsearch.hppc.CharArrayList;

import org.apache.lucene.analysis.charfilter.BaseCharFilter;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.Reader;

/**
 * Char filter for removing control chars from a stream
 */
public class ControlCharFilter extends BaseCharFilter {
    public static final String NAME = "control_char_filter";
    // TODO this is probably not ultimately necessary, keeping track of where we are in the stream
    // and optimizing our replacements (like MappingCharFilter), would be faster and use less memory
    private Reader transformedInput;

    public ControlCharFilter(Reader in) {
        super(in);
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
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
        CharArrayList charArrayList = new CharArrayList(1024);
        char[] temp = new char[1024];
        int totalRead = 0;
        int diff = 0;
        for (int cnt = input.read(temp); cnt > 0; cnt = input.read(temp)) {
            int pos = 0;
            while (pos < cnt) {
                int start = pos;
                while (start < cnt) {
                    if (isControlChar(temp[start]) == false) {
                        break;
                    }
                    start++;
                }
                if (start > pos) {
                    diff += (start - pos);
                    addOffCorrectMap(pos + totalRead, diff);
                }
                int size = 0;
                while (size < (cnt - start)) {
                    // While the category is not a control char; read.
                    if (isControlChar(temp[start + size]) == false) {
                        size++;
                    } else {
                        break;
                    }
                }
                charArrayList.add(temp, start, size);
                pos = start + size;
            }
            totalRead += cnt;
        }
        transformedInput = new CharArrayReader(charArrayList.toArray());
    }

    private static boolean isControlChar(char c) {
        if (c == '\n' || c == '\r' || c == '\t') {
            return false;
        }
        int category = Character.getType(c);

        return category >= Character.CONTROL && category <= Character.SURROGATE;
    }

}
