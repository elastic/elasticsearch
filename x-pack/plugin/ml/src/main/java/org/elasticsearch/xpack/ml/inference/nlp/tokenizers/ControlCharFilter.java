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
        for (int cnt = input.read(temp); cnt > 0; cnt = input.read(temp)) {
            int pos = 0;
            while (pos < cnt) {
                int start = pos;
                while (start < cnt) {
                    int category = Character.getType(temp[start]);
                    if (category < Character.CONTROL || category > Character.SURROGATE) {
                        break;
                    }
                    start++;
                }
                if (start > pos) {
                    addOffCorrectMap(pos, start);
                }
                int size = 0;
                while (size < (cnt - start)) {
                    int category = Character.getType(temp[start + size]);
                    if (category < Character.CONTROL || category > Character.SURROGATE) {
                        size++;
                    } else {
                        break;
                    }
                }
                charArrayList.add(temp, start, size);
                pos = start + size;
            }
        }
        transformedInput = new CharArrayReader(charArrayList.toArray());
    }

}
