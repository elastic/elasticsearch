/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class TaperedNGramTokenizerTests extends BaseTokenStreamTestCase {
    
    
    public void testLongString() throws IOException {
        checkTokens("Hello world", 6, "Hello ", "ello w", "llo wo", "lo wor", "o worl", " world", "world", "orld", "rld", "ld", "d");
    }

    public void testShortString() throws IOException {
        checkTokens("Hello", 5, "Hello", "ello", "llo", "lo", "o");
    }
    
    public void testSingleCharDoc() throws IOException {
        checkTokens("H", 5, "H");
    }

    public void testSingleCharNgram() throws IOException {
        checkTokens("Hello", 1, "H", "e", "l", "l", "o");
    }
    
    public void testFieldMapperEncoding() throws IOException {
        char TOKEN_START_OR_END_CHAR = 0;
        checkTokens(TOKEN_START_OR_END_CHAR+"aaa"+TOKEN_START_OR_END_CHAR, 5, 
                TOKEN_START_OR_END_CHAR+"aaa"+TOKEN_START_OR_END_CHAR, 
                "aaa"+TOKEN_START_OR_END_CHAR,
                "aa"+TOKEN_START_OR_END_CHAR, 
                "a"+TOKEN_START_OR_END_CHAR,
                ""+TOKEN_START_OR_END_CHAR);
    }    

    public void testTooShortNgram() {
        Exception expectedException = expectThrows(IllegalArgumentException.class, () -> checkTokens("Hello", 0, ""));
        assertThat(expectedException.getMessage(), equalTo("maxGram must be greater than zero"));

    }

    private static void checkTokens(String value, int ngramLength, String... expectedTokens) throws IOException {
        TaperedNgramTokenizer tokenizer = new TaperedNgramTokenizer(ngramLength);        
        tokenizer.setReader(new StringReader(value));      
        int [] posIncs = new int[expectedTokens.length];
        Arrays.fill(posIncs, 1);
        assertTokenStreamContents(tokenizer, expectedTokens, posIncs);
    }

}
