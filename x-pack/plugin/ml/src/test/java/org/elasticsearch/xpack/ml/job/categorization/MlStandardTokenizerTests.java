/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.categorization;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;

public class MlStandardTokenizerTests extends ESTestCase {

    public void testTokenizeNoPaths() throws IOException {
        String testData = "one .-_two **stars**in**their**eyes** three@-_ sand.-_wich 4four @five5 a1b2c3 42 www.elastic.co";
        try (Tokenizer tokenizer = new MlStandardTokenizer()) {
            tokenizer.setReader(new StringReader(testData));
            tokenizer.reset();
            CharTermAttribute term = tokenizer.addAttribute(CharTermAttribute.class);
            OffsetAttribute offset = tokenizer.addAttribute(OffsetAttribute.class);
            assertTrue(tokenizer.incrementToken());
            assertEquals("one", term.toString());
            assertEquals(0, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("two", term.toString());
            assertEquals(7, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("stars", term.toString());
            assertEquals(13, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("in", term.toString());
            assertEquals(20, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("their", term.toString());
            assertEquals(24, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("eyes", term.toString());
            assertEquals(31, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("three", term.toString());
            assertEquals(38, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("sand.-_wich", term.toString());
            assertEquals(47, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("five5", term.toString());
            assertEquals(66, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("www.elastic.co", term.toString());
            assertEquals(82, offset.startOffset());
            assertFalse(tokenizer.incrementToken());
            tokenizer.end();
            assertEquals(testData.length(), offset.endOffset());
        }
    }

    public void testTokenizeWithUnixPath() throws IOException {
        String testData = "Apr 27 21:04:05 Davids-MacBook-Pro com.apple.xpc.launchd[1] (com.apple.xpc.launchd.domain.pid.mdmclient.320): "
            + "Failed to bootstrap path: path = /usr/libexec/mdmclient, error = 108: Invalid path\n";
        try (Tokenizer tokenizer = new MlStandardTokenizer()) {
            tokenizer.setReader(new StringReader(testData));
            tokenizer.reset();
            CharTermAttribute term = tokenizer.addAttribute(CharTermAttribute.class);
            OffsetAttribute offset = tokenizer.addAttribute(OffsetAttribute.class);
            assertTrue(tokenizer.incrementToken());
            assertEquals("Apr", term.toString());
            assertEquals(0, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("Davids-MacBook-Pro", term.toString());
            assertEquals(16, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("com.apple.xpc.launchd", term.toString());
            assertEquals(35, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("com.apple.xpc.launchd.domain.pid.mdmclient.320", term.toString());
            assertEquals(61, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("Failed", term.toString());
            assertEquals(110, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("to", term.toString());
            assertEquals(117, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("bootstrap", term.toString());
            assertEquals(120, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("path", term.toString());
            assertEquals(130, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("path", term.toString());
            assertEquals(136, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("/usr/libexec/mdmclient", term.toString());
            assertEquals(143, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("error", term.toString());
            assertEquals(167, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("Invalid", term.toString());
            assertEquals(180, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("path", term.toString());
            assertEquals(188, offset.startOffset());
            assertFalse(tokenizer.incrementToken());
            tokenizer.end();
            assertEquals(testData.length(), offset.endOffset());
        }
    }

    public void testTokenizeWithWindowsPaths() throws IOException {
        String testData = "05/05/2021 C:\\Windows\\System something:else \\\\myserver\\share\\folder \\ at_end\\ \\no\\drive\\specified "
            + "at_end_with_colon:\\";
        try (Tokenizer tokenizer = new MlStandardTokenizer()) {
            tokenizer.setReader(new StringReader(testData));
            tokenizer.reset();
            CharTermAttribute term = tokenizer.addAttribute(CharTermAttribute.class);
            OffsetAttribute offset = tokenizer.addAttribute(OffsetAttribute.class);
            assertTrue(tokenizer.incrementToken());
            assertEquals("C:\\Windows\\System", term.toString());
            assertEquals(11, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("something", term.toString());
            assertEquals(29, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("else", term.toString());
            assertEquals(39, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("\\\\myserver\\share\\folder", term.toString());
            assertEquals(44, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("at_end", term.toString());
            assertEquals(70, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("\\no\\drive\\specified", term.toString());
            assertEquals(78, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("at_end_with_colon", term.toString());
            assertEquals(98, offset.startOffset());
            assertFalse(tokenizer.incrementToken());
            tokenizer.end();
            assertEquals(testData.length(), offset.endOffset());
        }
    }

    public void testTokenizeWithUrl() throws IOException {
        String testData = """
            10.8.0.12 - - [29/Nov/2020:21:34:55 +0000] "POST /intake/v2/events HTTP/1.1" 202 0 "-" \
            "elasticapm-dotnet/1.5.1 System.Net.Http/4.6.28208.02 .NET_Core/2.2.8" 27821 0.002 [default-apm-apm-server-8200] [] \
            10.8.1.19:8200 0 0.001 202 f961c776ff732f5c8337530aa22c7216
            """;
        try (Tokenizer tokenizer = new MlStandardTokenizer()) {
            tokenizer.setReader(new StringReader(testData));
            tokenizer.reset();
            CharTermAttribute term = tokenizer.addAttribute(CharTermAttribute.class);
            OffsetAttribute offset = tokenizer.addAttribute(OffsetAttribute.class);
            assertTrue(tokenizer.incrementToken());
            assertEquals("POST", term.toString());
            assertEquals(44, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("/intake/v2/events", term.toString());
            assertEquals(49, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("HTTP/1.1", term.toString());
            assertEquals(67, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("elasticapm-dotnet/1.5.1", term.toString());
            assertEquals(88, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("System.Net.Http/4.6.28208.02", term.toString());
            assertEquals(112, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("NET_Core/2.2.8", term.toString());
            assertEquals(142, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("default-apm-apm-server-8200", term.toString());
            assertEquals(171, offset.startOffset());
            assertFalse(tokenizer.incrementToken());
            tokenizer.end();
            assertEquals(testData.length(), offset.endOffset());
        }
    }

    public void testTokenizeWithPurelyNumericAtToken() throws IOException {
        String testData =
            "[1231529792] INFO  proxy <12309105041220090733@192.168.123.123> - +++++++++++++++ CREATING ProxyCore ++++++++++++++++";
        try (Tokenizer tokenizer = new MlStandardTokenizer()) {
            tokenizer.setReader(new StringReader(testData));
            tokenizer.reset();
            CharTermAttribute term = tokenizer.addAttribute(CharTermAttribute.class);
            OffsetAttribute offset = tokenizer.addAttribute(OffsetAttribute.class);
            assertTrue(tokenizer.incrementToken());
            assertEquals("INFO", term.toString());
            assertEquals(13, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("proxy", term.toString());
            assertEquals(19, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("CREATING", term.toString());
            assertEquals(82, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("ProxyCore", term.toString());
            assertEquals(91, offset.startOffset());
            assertFalse(tokenizer.incrementToken());
            tokenizer.end();
            assertEquals(testData.length(), offset.endOffset());
        }
    }

    public void testTokenizeWithEmailAddress() throws IOException {
        String testData = "[1234662464] INFO  session <a.n.other@elastic.co> - -----------------";
        try (Tokenizer tokenizer = new MlStandardTokenizer()) {
            tokenizer.setReader(new StringReader(testData));
            tokenizer.reset();
            CharTermAttribute term = tokenizer.addAttribute(CharTermAttribute.class);
            OffsetAttribute offset = tokenizer.addAttribute(OffsetAttribute.class);
            assertTrue(tokenizer.incrementToken());
            assertEquals("INFO", term.toString());
            assertEquals(13, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("session", term.toString());
            assertEquals(19, offset.startOffset());
            assertTrue(tokenizer.incrementToken());
            assertEquals("a.n.other@elastic.co", term.toString());
            assertEquals(28, offset.startOffset());
            assertFalse(tokenizer.incrementToken());
            tokenizer.end();
            assertEquals(testData.length(), offset.endOffset());
        }
    }

    public void testTokenize_emptyString() throws IOException {
        String testData = "";
        try (Tokenizer tokenizer = new MlStandardTokenizer()) {
            tokenizer.setReader(new StringReader(testData));
            tokenizer.reset();
            CharTermAttribute term = tokenizer.addAttribute(CharTermAttribute.class);
            OffsetAttribute offset = tokenizer.addAttribute(OffsetAttribute.class);
            assertFalse(tokenizer.incrementToken());
            assertEquals("", term.toString());
            tokenizer.end();
            assertEquals(testData.length(), offset.endOffset());
        }
    }
}
