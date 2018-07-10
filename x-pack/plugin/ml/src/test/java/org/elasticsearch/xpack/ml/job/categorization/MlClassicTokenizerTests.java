/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.categorization;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;


public class MlClassicTokenizerTests extends ESTestCase {

    public void testTokenize() throws IOException {
        String testData = "one .-_two **stars**in**their**eyes** three.-_ sand.-_wich 4four five5 a1b2c3 42 www.elastic.co";
        try (Tokenizer tokenizer = new MlClassicTokenizer()) {
            tokenizer.setReader(new StringReader(testData));
            tokenizer.reset();
            CharTermAttribute term = tokenizer.addAttribute(CharTermAttribute.class);
            assertTrue(tokenizer.incrementToken());
            assertEquals("one", term.toString());
            assertTrue(tokenizer.incrementToken());
            assertEquals("two", term.toString());
            assertTrue(tokenizer.incrementToken());
            assertEquals("stars", term.toString());
            assertTrue(tokenizer.incrementToken());
            assertEquals("in", term.toString());
            assertTrue(tokenizer.incrementToken());
            assertEquals("their", term.toString());
            assertTrue(tokenizer.incrementToken());
            assertEquals("eyes", term.toString());
            assertTrue(tokenizer.incrementToken());
            assertEquals("three", term.toString());
            assertTrue(tokenizer.incrementToken());
            assertEquals("sand.-_wich", term.toString());
            assertTrue(tokenizer.incrementToken());
            assertEquals("five5", term.toString());
            assertTrue(tokenizer.incrementToken());
            assertEquals("www.elastic.co", term.toString());
            assertFalse(tokenizer.incrementToken());
            tokenizer.end();
        }
    }

    public void testTokenize_emptyString() throws IOException {
        String testData = "";
        try (Tokenizer tokenizer = new MlClassicTokenizer()) {
            tokenizer.setReader(new StringReader(testData));
            tokenizer.reset();
            CharTermAttribute term = tokenizer.addAttribute(CharTermAttribute.class);
            assertFalse(tokenizer.incrementToken());
            assertEquals("", term.toString());
            tokenizer.end();
        }
    }
}
