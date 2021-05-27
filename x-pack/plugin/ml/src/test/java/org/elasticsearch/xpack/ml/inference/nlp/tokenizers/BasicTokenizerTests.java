/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Some test cases taken from
 * https://github.com/huggingface/transformers/blob/ba8c4d0ac04acfcdbdeaed954f698d6d5ec3e532/tests/test_tokenization_bert.py
 */
public class BasicTokenizerTests extends ESTestCase {

    public void testLowerCase() {
        BasicTokenizer tokenizer = new BasicTokenizer();
        List<String> tokens = tokenizer.tokenize(" \tHeLLo!how  \n Are yoU?  ");
        assertThat(tokens, contains("hello", "!", "how", "are", "you", "?"));

        tokens = tokenizer.tokenize("H\u00E9llo");
        assertThat(tokens, contains("hello"));
    }

    public void testLowerCaseWithoutStripAccents() {
        BasicTokenizer tokenizer = new BasicTokenizer(true, true, false);
        List<String> tokens = tokenizer.tokenize(" \tHäLLo!how  \n Are yoU?  ");
        assertThat(tokens, contains("hällo", "!", "how", "are", "you", "?"));

        tokens = tokenizer.tokenize("H\u00E9llo");
        assertThat(tokens, contains("h\u00E9llo"));
    }

    public void testLowerCaseStripAccentsDefault() {
        BasicTokenizer tokenizer = new BasicTokenizer(true, true);
        List<String> tokens = tokenizer.tokenize(" \tHäLLo!how  \n Are yoU?  ");
        assertThat(tokens, contains("hallo", "!", "how", "are", "you", "?"));

        tokens = tokenizer.tokenize("H\u00E9llo");
        assertThat(tokens, contains("hello"));
    }

    public void testNoLower() {
        List<String> tokens = new BasicTokenizer(false, true, false).tokenize(" \tHäLLo!how  \n Are yoU?  ");
        assertThat(tokens, contains("HäLLo", "!", "how", "Are", "yoU", "?"));
    }

    public void testNoLowerStripAccents() {
        List<String> tokens = new BasicTokenizer(false, true, true).tokenize(" \tHäLLo!how  \n Are yoU?  ");
        assertThat(tokens, contains("HaLLo", "!", "how", "Are", "yoU", "?"));
    }

    public void testNeverSplit() {
        BasicTokenizer tokenizer = new BasicTokenizer(false, false, false, Collections.singleton("[UNK]"));
        List<String> tokens = tokenizer.tokenize(" \tHeLLo!how  \n Are yoU? [UNK]");
        assertThat(tokens, contains("HeLLo", "!", "how", "Are", "yoU", "?", "[UNK]"));

        tokens = tokenizer.tokenize("Hello [UNK].");
        assertThat(tokens, contains("Hello", "[UNK]", "."));

        tokens = tokenizer.tokenize("Hello [UNK]?");
        assertThat(tokens, contains("Hello", "[UNK]", "?"));
    }

    public void testSplitOnPunctuation() {
        List<String> tokens = BasicTokenizer.splitOnPunctuation("hi!");
        assertThat(tokens, contains("hi", "!"));

        tokens = BasicTokenizer.splitOnPunctuation("hi.");
        assertThat(tokens, contains("hi", "."));

        tokens = BasicTokenizer.splitOnPunctuation("!hi");
        assertThat(tokens, contains("!", "hi"));

        tokens = BasicTokenizer.splitOnPunctuation("don't");
        assertThat(tokens, contains("don", "'", "t"));

        tokens = BasicTokenizer.splitOnPunctuation("!!hi");
        assertThat(tokens, contains("!", "!", "hi"));

        tokens = BasicTokenizer.splitOnPunctuation("[hi]");
        assertThat(tokens, contains("[", "hi", "]"));

        tokens = BasicTokenizer.splitOnPunctuation("hi.");
        assertThat(tokens, contains("hi", "."));
    }

    public void testStripAccents() {
        assertEquals("Hallo", BasicTokenizer.stripAccents("Hällo"));
    }

    public void testTokenizeCjkChars() {
        assertEquals(" \u535A  \u63A8 ", BasicTokenizer.tokenizeCjkChars("\u535A\u63A8"));

        String noCjkChars = "hello";
        assertThat(BasicTokenizer.tokenizeCjkChars(noCjkChars), sameInstance(noCjkChars));
    }

    public void testTokenizeChinese() {
        List<String> tokens = new BasicTokenizer().tokenize("ah\u535A\u63A8zz");
        assertThat(tokens, contains("ah", "\u535A", "\u63A8", "zz"));
    }

    public void testCleanText() {
        assertEquals("change these chars to spaces",
            BasicTokenizer.cleanText("change\tthese chars\rto\nspaces"));
        assertEquals("filter control chars",
            BasicTokenizer.cleanText("\u0000filter \uFFFDcontrol chars\u0005"));
    }

    public void testWhiteSpaceTokenize() {
        assertThat(BasicTokenizer.whiteSpaceTokenize("nochange"), arrayContaining("nochange"));
        assertThat(BasicTokenizer.whiteSpaceTokenize(" some  change "), arrayContaining("some", "", "change"));
    }

    public void testIsWhitespace() {
        assertTrue(BasicTokenizer.isWhiteSpace(' '));
        assertTrue(BasicTokenizer.isWhiteSpace('\t'));
        assertTrue(BasicTokenizer.isWhiteSpace('\r'));
        assertTrue(BasicTokenizer.isWhiteSpace('\n'));
        assertTrue(BasicTokenizer.isWhiteSpace('\u00A0'));

        assertFalse(BasicTokenizer.isWhiteSpace('_'));
        assertFalse(BasicTokenizer.isWhiteSpace('A'));
    }

    public void testIsControl() {
        assertTrue(BasicTokenizer.isControlChar('\u0005'));
        assertTrue(BasicTokenizer.isControlChar('\u001C'));

        assertFalse(BasicTokenizer.isControlChar('A'));
        assertFalse(BasicTokenizer.isControlChar(' '));
        assertFalse(BasicTokenizer.isControlChar('\t'));
        assertFalse(BasicTokenizer.isControlChar('\r'));
    }

    public void testIsPunctuation() {
        assertTrue(BasicTokenizer.isCommonPunctuation('-'));
        assertTrue(BasicTokenizer.isCommonPunctuation('$'));
        assertTrue(BasicTokenizer.isCommonPunctuation('.'));
        assertFalse(BasicTokenizer.isCommonPunctuation(' '));
        assertFalse(BasicTokenizer.isCommonPunctuation('A'));
        assertFalse(BasicTokenizer.isCommonPunctuation('`'));

        assertTrue(BasicTokenizer.isPunctuationMark('-'));
        assertTrue(BasicTokenizer.isPunctuationMark('$'));
        assertTrue(BasicTokenizer.isPunctuationMark('`'));
        assertTrue(BasicTokenizer.isPunctuationMark('.'));
        assertFalse(BasicTokenizer.isPunctuationMark(' '));
        assertFalse(BasicTokenizer.isPunctuationMark('A'));

        assertFalse(BasicTokenizer.isCommonPunctuation('['));
        assertTrue(BasicTokenizer.isPunctuationMark('['));
    }

    public void testIsCjkChar() {
        assertTrue(BasicTokenizer.isCjkChar(0x3400));
        assertFalse(BasicTokenizer.isCjkChar(0x4DC0));

        assertTrue(BasicTokenizer.isCjkChar(0xF900));
        assertFalse(BasicTokenizer.isCjkChar(0xFB00));

        assertTrue(BasicTokenizer.isCjkChar(0x20000));
        assertFalse(BasicTokenizer.isCjkChar(0x2A6E0));

        assertTrue(BasicTokenizer.isCjkChar(0x20000));
        assertFalse(BasicTokenizer.isCjkChar(0x2A6E0));

        assertTrue(BasicTokenizer.isCjkChar(0x2A700));
        assertFalse(BasicTokenizer.isCjkChar(0x2CEB0));

        assertTrue(BasicTokenizer.isCjkChar(0x2F800));
        assertFalse(BasicTokenizer.isCjkChar(0x2FA20));
    }
}
