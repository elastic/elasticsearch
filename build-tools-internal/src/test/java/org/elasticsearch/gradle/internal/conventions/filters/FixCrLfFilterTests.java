/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions.filters;

import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.elasticsearch.gradle.internal.conventions.filters.ReplaceTokensTests.readFully;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class FixCrLfFilterTests {

    @Test
    public void convertsAllLineEndingsToCrLf() throws IOException {
        assertEquals("a\r\nb\r\nc\r\nd\r\n", filter("a\nb\r\nc\rd\n", "crlf"));
    }

    @Test
    public void convertsToLf() throws IOException {
        assertEquals("a\nb\nc\n", filter("a\r\nb\rc\n", "lf"));
        assertEquals("a\nb\n", filter("a\r\nb", "unix"));
    }

    @Test
    public void keepsExistingCrLf() throws IOException {
        assertEquals("@echo off\r\nset A=1\r\n", filter("@echo off\r\nset A=1\r\n", "crlf"));
    }

    @Test
    public void addsMissingFinalLineEnding() throws IOException {
        assertEquals("a\r\nb\r\n", filter("a\nb", "crlf"));
        // like Ant, an empty file gets a single line ending
        assertEquals("\r\n", filter("", "crlf"));
    }

    @Test
    public void handlesDoubleCarriageReturnsLikeAnt() throws IOException {
        // "\r\r\n" is a single line ending, "\r\r" two
        assertEquals("a\r\nb\r\n", filter("a\r\r\nb", "crlf"));
        assertEquals("a\r\n\r\nb\r\n", filter("a\r\rb", "crlf"));
        assertEquals("a\r\n\r\n\r\n", filter("a\r\r\r\n", "crlf"));
    }

    @Test
    public void removesTrailingCtrlZ() throws IOException {
        assertEquals("a\r\n", filter("a\n\u001A", "crlf"));
        assertEquals("a\u001Ab\r\n", filter("a\u001Ab", "crlf"));
        // only one trailing Ctrl-Z is removed; a remaining one stays last, after the added line ending
        assertEquals("a\r\n\u001A", filter("a\u001A\u001A", "crlf"));
        assertEquals("a\r\n\u001A", filter("a\n\u001A\u001A", "crlf"));
    }

    @Test
    public void leavesTabsAndContentUntouched() throws IOException {
        assertEquals("\tset \"X=\u00e9\"\r\n", filter("\tset \"X=\u00e9\"\n", "crlf"));
    }

    @Test
    public void defaultsToLf() throws IOException {
        assertEquals("a\nb\n", readFully(new FixCrLfFilter(new StringReader("a\r\nb"))));
    }

    @Test
    public void rejectsUnknownEol() {
        assertThrows(IllegalArgumentException.class, () -> new FixCrLfFilter(new StringReader("")).setEol("asis"));
    }

    private static String filter(String content, String eol) throws IOException {
        FixCrLfFilter filter = new FixCrLfFilter(new StringReader(content));
        filter.setEol(eol);
        return readFully(filter);
    }
}
