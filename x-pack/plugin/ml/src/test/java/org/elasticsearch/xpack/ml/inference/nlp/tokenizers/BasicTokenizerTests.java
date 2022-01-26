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
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Some test cases taken from
 * https://github.com/huggingface/transformers/blob/ba8c4d0ac04acfcdbdeaed954f698d6d5ec3e532/tests/test_tokenization_bert.py
 */
public class BasicTokenizerTests extends ESTestCase {

    public void testLowerCase() {
        BasicTokenizer tokenizer = new BasicTokenizer();
        var tokens = tokenizer.tokenize(" \tHeLLo!how  \n Are yoU?  ");
        assertThat(tokenStrings(tokens), contains("hello", "!", "how", "are", "you", "?"));

        tokens = tokenizer.tokenize("H\u00E9llo");
        assertThat(tokenStrings(tokens), contains("hello"));
    }

    public void testLowerCaseWithoutStripAccents() {
        BasicTokenizer tokenizer = new BasicTokenizer(true, true, false);
        var tokens = tokenizer.tokenize(" \tHäLLo!how  \n Are yoU?  ");
        assertThat(tokenStrings(tokens), contains("hällo", "!", "how", "are", "you", "?"));

        tokens = tokenizer.tokenize("H\u00E9llo");
        assertThat(tokenStrings(tokens), contains("h\u00E9llo"));
    }

    public void testLowerCaseStripAccentsDefault() {
        BasicTokenizer tokenizer = new BasicTokenizer(true, true);
        var tokens = tokenizer.tokenize(" \tHäLLo!how  \n Are yoU?  ");
        assertThat(tokenStrings(tokens), contains("hallo", "!", "how", "are", "you", "?"));

        tokens = tokenizer.tokenize("H\u00E9llo");
        assertThat(tokenStrings(tokens), contains("hello"));
    }

    public void testNoLower() {
        var tokens = new BasicTokenizer(false, true, false).tokenize(" \tHäLLo!how  \n Are yoU?  ");
        assertThat(tokenStrings(tokens), contains("HäLLo", "!", "how", "Are", "yoU", "?"));
    }

    public void testNoLowerStripAccents() {
        var tokens = new BasicTokenizer(false, true, true).tokenize(" \tHäLLo!how  \n Are yoU?  ");
        assertThat(tokenStrings(tokens), contains("HaLLo", "!", "how", "Are", "yoU", "?"));
    }

    public void testNeverSplit_GivenNoLowerCase() {
        BasicTokenizer tokenizer = new BasicTokenizer(false, false, false, Collections.singleton("[UNK]"));
        var tokens = tokenizer.tokenize(" \tHeLLo!how  \n Are yoU? [UNK]");
        assertThat(tokenStrings(tokens), contains("HeLLo", "!", "how", "Are", "yoU", "?", "[UNK]"));

        tokens = tokenizer.tokenize("Hello [UNK].");
        assertThat(tokenStrings(tokens), contains("Hello", "[UNK]", "."));

        tokens = tokenizer.tokenize("Hello [UNK]?");
        assertThat(tokenStrings(tokens), contains("Hello", "[UNK]", "?"));

        tokens = tokenizer.tokenize("Hello [UNK]!!");
        assertThat(tokenStrings(tokens), contains("Hello", "[UNK]", "!", "!"));

        tokens = tokenizer.tokenize("Hello-[UNK]");
        assertThat(tokenStrings(tokens), contains("Hello", "-", "[UNK]"));
        tokens = tokenizer.tokenize("Hello~[UNK][UNK]");
        assertThat(tokenStrings(tokens), contains("Hello", "~", "[UNK]", "[UNK]"));
        assertThat(tokenStrings(tokenizer.tokenize("Hello~[[UNK]")), contains("Hello", "~", "[", "[UNK]"));
        assertThat(tokenStrings(tokenizer.tokenize("Hello~[[[UNK]")), contains("Hello", "~", "[", "[", "[UNK]"));
        assertThat(tokenStrings(tokenizer.tokenize("Hello~[UNK]]")), contains("Hello", "~", "[UNK]", "]"));
        assertThat(tokenStrings(tokenizer.tokenize("Hello~[UNK]]]")), contains("Hello", "~", "[UNK]", "]", "]"));
        assertThat(tokenStrings(tokenizer.tokenize("Hello~[[UNK]]")), contains("Hello", "~", "[", "[UNK]", "]"));
        tokens = tokenizer.tokenize("Hello-[unk]");
        assertThat(tokenStrings(tokens), contains("Hello", "-", "[", "unk", "]"));
    }

    public void testNeverSplit_GivenLowerCase() {
        BasicTokenizer tokenizer = new BasicTokenizer(true, false, false, Collections.singleton("[UNK]"));
        var tokens = tokenizer.tokenize(" \tHeLLo!how  \n Are yoU? [UNK]");
        assertThat(tokenStrings(tokens), contains("hello", "!", "how", "are", "you", "?", "[UNK]"));

        tokens = tokenizer.tokenize("Hello [UNK].");
        assertThat(tokenStrings(tokens), contains("hello", "[UNK]", "."));

        tokens = tokenizer.tokenize("Hello [UNK]?");
        assertThat(tokenStrings(tokens), contains("hello", "[UNK]", "?"));

        tokens = tokenizer.tokenize("Hello [UNK]!!");
        assertThat(tokenStrings(tokens), contains("hello", "[UNK]", "!", "!"));

        tokens = tokenizer.tokenize("Hello-[UNK]");
        assertThat(tokenStrings(tokens), contains("hello", "-", "[UNK]"));
        tokens = tokenizer.tokenize("Hello~[UNK][UNK]");
        assertThat(tokenStrings(tokens), contains("hello", "~", "[UNK]", "[UNK]"));
        tokens = tokenizer.tokenize("Hello-[unk]");
        assertThat(tokenStrings(tokens), contains("hello", "-", "[", "unk", "]"));
    }

    public void testSplitOnPunctuation() {
        var tokens = BasicTokenizer.splitOnPunctuation(new DelimitedToken(0, 3, "hi!"));
        assertEquals(new DelimitedToken(0, 2, "hi"), tokens.get(0));
        assertEquals(new DelimitedToken(2, 3, "!"), tokens.get(1));

        tokens = BasicTokenizer.splitOnPunctuation(makeToken("hi."));
        assertEquals(new DelimitedToken(0, 2, "hi"), tokens.get(0));
        assertEquals(new DelimitedToken(2, 3, "."), tokens.get(1));

        tokens = BasicTokenizer.splitOnPunctuation(makeToken("!hi"));
        assertEquals(new DelimitedToken(0, 1, "!"), tokens.get(0));
        assertEquals(new DelimitedToken(1, 3, "hi"), tokens.get(1));

        tokens = BasicTokenizer.splitOnPunctuation(makeToken("don't"));
        assertEquals(new DelimitedToken(0, 3, "don"), tokens.get(0));
        assertEquals(new DelimitedToken(3, 4, "'"), tokens.get(1));
        assertEquals(new DelimitedToken(4, 5, "t"), tokens.get(2));

        tokens = BasicTokenizer.splitOnPunctuation(makeToken("!!hi"));
        assertEquals(new DelimitedToken(0, 1, "!"), tokens.get(0));
        assertEquals(new DelimitedToken(1, 2, "!"), tokens.get(1));
        assertEquals(new DelimitedToken(2, 4, "hi"), tokens.get(2));

        tokens = BasicTokenizer.splitOnPunctuation(makeToken("[hi]"));
        assertEquals(new DelimitedToken(0, 1, "["), tokens.get(0));
        assertEquals(new DelimitedToken(1, 3, "hi"), tokens.get(1));
        assertEquals(new DelimitedToken(3, 4, "]"), tokens.get(2));

        tokens = BasicTokenizer.splitOnPunctuation(makeToken("!!"));
        assertEquals(new DelimitedToken(0, 1, "!"), tokens.get(0));
        assertEquals(new DelimitedToken(1, 2, "!"), tokens.get(1));

        tokens = BasicTokenizer.splitOnPunctuation(makeToken("elastic’s"));
        assertEquals(new DelimitedToken(0, 7, "elastic"), tokens.get(0));
        assertEquals(new DelimitedToken(7, 8, "’"), tokens.get(1));
        assertEquals(new DelimitedToken(8, 9, "s"), tokens.get(2));

        tokens = BasicTokenizer.splitOnPunctuation(new DelimitedToken(4, 13, "elastic’s"));
        assertEquals(new DelimitedToken(4, 11, "elastic"), tokens.get(0));
        assertEquals(new DelimitedToken(11, 12, "’"), tokens.get(1));
        assertEquals(new DelimitedToken(12, 13, "s"), tokens.get(2));
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
        var tokens = new BasicTokenizer().tokenize("ah\u535A\u63A8zz");
        assertThat(tokenStrings(tokens), contains("ah", "\u535A", "\u63A8", "zz"));
    }

    public void testCleanText() {
        assertEquals("change these chars to spaces", BasicTokenizer.cleanText("change\tthese chars\rto\nspaces"));
        assertEquals("filter control chars", BasicTokenizer.cleanText("\u0000filter \uFFFDcontrol chars\u0005"));
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
        assertTrue(BasicTokenizer.isPunctuationMark('-'));
        assertTrue(BasicTokenizer.isPunctuationMark('$'));
        assertTrue(BasicTokenizer.isPunctuationMark('`'));
        assertTrue(BasicTokenizer.isPunctuationMark('.'));
        assertFalse(BasicTokenizer.isPunctuationMark(' '));
        assertFalse(BasicTokenizer.isPunctuationMark('A'));
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

    public void testWhitespaceTokenize() {
        {
            List<DelimitedToken> delimitedTokens = BasicTokenizer.whiteSpaceTokenize("hello! how are you?");
            assertThat(delimitedTokens, hasSize(4));
            assertThat(tokenStrings(delimitedTokens), contains("hello!", "how", "are", "you?"));

            assertThat(delimitedTokens.get(0), equalTo(new DelimitedToken(0, 6, "hello!")));
            assertThat(delimitedTokens.get(1), equalTo(new DelimitedToken(7, 10, "how")));
            assertThat(delimitedTokens.get(2), equalTo(new DelimitedToken(11, 14, "are")));
            assertThat(delimitedTokens.get(3), equalTo(new DelimitedToken(15, 19, "you?")));
        }
        {
            List<DelimitedToken> delimitedTokens = BasicTokenizer.whiteSpaceTokenize("   leading whitespace");
            assertThat(delimitedTokens, hasSize(2));
            assertThat(tokenStrings(delimitedTokens), contains("leading", "whitespace"));

            assertThat(delimitedTokens.get(0), equalTo(new DelimitedToken(3, 10, "leading")));
            assertThat(delimitedTokens.get(1), equalTo(new DelimitedToken(11, 21, "whitespace")));
        }
        {
            List<DelimitedToken> delimitedTokens = BasicTokenizer.whiteSpaceTokenize("double  spaced  text ");
            assertThat(delimitedTokens, hasSize(3));
            assertThat(tokenStrings(delimitedTokens), contains("double", "spaced", "text"));

            assertThat(delimitedTokens.get(0), equalTo(new DelimitedToken(0, 6, "double")));
            assertThat(delimitedTokens.get(1), equalTo(new DelimitedToken(8, 14, "spaced")));
            assertThat(delimitedTokens.get(2), equalTo(new DelimitedToken(16, 20, "text")));
        }
    }

    private List<String> tokenStrings(List<DelimitedToken> tokens) {
        return tokens.stream().map(DelimitedToken::getToken).collect(Collectors.toList());
    }

    private DelimitedToken makeToken(String str) {
        return new DelimitedToken(0, str.length(), str);
    }

}
