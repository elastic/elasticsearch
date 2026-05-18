/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;

import java.io.CharArrayReader;
import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ControlCharFilterTests extends ESTestCase {

    public void testOnlyControlChars() throws IOException {
        ControlCharFilter controlCharFilter = new ControlCharFilter(
            new CharArrayReader(new char[] { Character.FORMAT, Character.CONTROL, Character.CONTROL, Character.CONTROL })
        );
        char[] output = new char[10];
        assertThat(controlCharFilter.read(output, 0, 5), equalTo(-1));
    }

    public void testEmpty() throws IOException {
        ControlCharFilter controlCharFilter = new ControlCharFilter(new CharArrayReader(new char[] {}));
        char[] output = new char[10];
        assertThat(controlCharFilter.read(output, 0, 5), equalTo(-1));
    }

    public void testCorrect() throws IOException {
        ControlCharFilter controlCharFilter = new ControlCharFilter(
            new CharArrayReader(
                new char[] {
                    Character.FORMAT,
                    Character.FORMAT,
                    'a',
                    Character.FORMAT,
                    Character.FORMAT,
                    'b',
                    'b',
                    Character.CONTROL,
                    'c',
                    'c',
                    Character.CONTROL,
                    Character.CONTROL }
            )
        );
        char[] output = new char[10];
        int read = controlCharFilter.read(output, 0, 10);
        assertThat(read, equalTo(5));
        assertThat(new String(output, 0, read), equalTo("abbcc"));
    }

    public void testCorrectForLongString() throws IOException {
        char[] charArray = new char[2000];
        int i = 0;
        for (; i < 1000; i++) {
            charArray[i] = 'a';
        }
        charArray[i++] = Character.CONTROL;
        charArray[i++] = Character.CONTROL;
        for (int j = 0; j < 997; j++) {
            charArray[i++] = 'a';
        }
        charArray[i] = Character.CONTROL;
        ControlCharFilter controlCharFilter = new ControlCharFilter(new CharArrayReader(charArray));
        char[] output = new char[2000];
        int read = controlCharFilter.read(output);
        assertThat(read, equalTo(1997));
        for (int j = 0; j < read; j++) {
            assertEquals('a', output[j]);
        }
    }

}
