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
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ReplaceTokensTests {

    @Test
    public void replacesKnownTokens() throws IOException {
        assertEquals("version 1.0 of es", filter("version @version@ of @name@", Map.of("version", "1.0", "name", "es")));
    }

    @Test
    public void leavesUnknownTokensUntouched() throws IOException {
        assertEquals("@unknown@ and 1.0", filter("@unknown@ and @version@", Map.of("version", "1.0")));
    }

    @Test
    public void unknownTokenDoesNotConsumeClosingDelimiter() throws IOException {
        // like Ant, matching restarts at the second '@' so the adjacent known token is still replaced
        assertEquals("@foo1.0", filter("@foo@version@", Map.of("version", "1.0")));
        assertEquals("1.01.0", filter("@version@@version@", Map.of("version", "1.0")));
    }

    @Test
    public void doesNotRescanReplacementValues() throws IOException {
        assertEquals("@b@", filter("@a@", Map.of("a", "@b@", "b", "nope")));
    }

    @Test
    public void handlesLoneDelimitersAndTokensAtBoundaries() throws IOException {
        Map<String, String> tokens = Map.of("x", "y");
        assertEquals("y", filter("@x@", tokens));
        assertEquals("a@", filter("a@", tokens));
        assertEquals("@", filter("@", tokens));
        assertEquals("@x", filter("@x", tokens));
        assertEquals("user@example.com y", filter("user@example.com @x@", tokens));
        assertEquals("", filter("", tokens));
    }

    @Test
    public void prefersShortestMatchingToken() throws IOException {
        Map<String, String> tokens = new LinkedHashMap<>();
        tokens.put("a@b", "long");
        tokens.put("a", "short");
        assertEquals("shortb@", filter("@a@b@", tokens));
    }

    @Test
    public void convertsValuesToStrings() throws IOException {
        assertEquals("count: 3, enabled: true", filter("count: @count@, enabled: @enabled@", Map.of("count", 3, "enabled", true)));
    }

    @Test
    public void passesContentThroughWithoutTokens() throws IOException {
        String content = "no tokens here\r\nunicode: \u00e9\u00e8 \uD83D\uDE00\n";
        assertEquals(content, filter(content, Map.of("x", "y")));
        assertEquals(content, filter(content, Map.of()));
    }

    @Test
    public void supportsSingleCharacterReads() throws IOException {
        ReplaceTokens filter = new ReplaceTokens(new StringReader("a @x@ b"));
        filter.setTokens(Map.of("x", "yz"));
        StringBuilder result = new StringBuilder();
        int c;
        while ((c = filter.read()) != -1) {
            result.append((char) c);
        }
        assertEquals("a yz b", result.toString());
    }

    @Test
    public void rejectsReadsAfterClose() throws IOException {
        ReplaceTokens filter = new ReplaceTokens(new StringReader("a @x@ b"));
        filter.setTokens(Map.of("x", "yz"));
        assertEquals('a', filter.read());
        filter.close();
        assertThrows(IOException.class, filter::read);
        assertThrows(IOException.class, () -> filter.read(new char[4], 0, 4));
        assertThrows(IOException.class, () -> filter.skip(1));
        assertThrows(IOException.class, filter::ready);
        // closing twice is a no-op
        filter.close();
    }

    private static String filter(String content, Map<String, ?> tokens) throws IOException {
        ReplaceTokens filter = new ReplaceTokens(new StringReader(content));
        filter.setTokens(tokens);
        return readFully(filter);
    }

    static String readFully(Reader reader) throws IOException {
        StringWriter writer = new StringWriter();
        reader.transferTo(writer);
        return writer.toString();
    }
}
