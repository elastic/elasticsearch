/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.equalTo;

public class TaperedNgramTokenFilterTests extends ESTestCase {
    
    
    public void testLongString() {
        checkTokens("Hello world", 6, "Hello ", "ello w", "llo wo", "lo wor", "o worl", " world", "world", "orld", "rld", "ld", "d");
    }

    public void testShortString() {
        checkTokens("Hello", 5, "Hello", "ello", "llo", "lo", "o");
    }
    
    public void testSingleCharDoc() {
        checkTokens("H", 5, "H");
    }

    public void testSingleCharNgram() {
        checkTokens("Hello", 1, "H", "e", "l", "l", "o");
    }
    
    public void testFieldMapperEncoding() {
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

    private static void checkTokens(String value, int ngramLength, String... expectedTokens) {
        KeywordTokenizer kt = new KeywordTokenizer(256);
        kt.setReader(new StringReader(value));
        TokenFilter filter = new TaperedNgramTokenFilter(kt, ngramLength);
        CharTermAttribute termAtt = filter.addAttribute(CharTermAttribute.class);
        int tokPos = 0;
        try {
            filter.reset();
            while (filter.incrementToken()) {
                String expectedToken = expectedTokens[tokPos++];
                String actualToken = termAtt.toString();
                assertEquals(expectedToken, actualToken);
            }
            kt.end();
            kt.close();
        } catch (IOException ioe) {
            throw new ElasticsearchParseException("Error parsing wildcard query pattern fragment [" + value + "]");
        }
        assertEquals(expectedTokens.length, tokPos);
    }

}
