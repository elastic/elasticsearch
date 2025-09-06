/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class PatternUtilsParseMultiTokenFormatTests extends ESTestCase {

    private List<TokenType> tokenTypes;
    private Set<Character> boundaryChars;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.tokenTypes = createTestTokenTypes();
        this.boundaryChars = Schema.getInstance().getTokenBoundaryChars();
    }

    private List<TokenType> createTestTokenTypes() {
        List<TokenType> tokenTypes = new ArrayList<>();

        // Create mock SubTokenType arrays for TokenFormat
        SubTokenType[] mockSubTokens = new SubTokenType[1];
        mockSubTokens[0] = createMockSubTokenType("mockSubToken");

        TokenFormat mockFormat = new TokenFormat("$mockSubToken", new char[0], mockSubTokens);

        tokenTypes.add(new TokenType("time", EncodingType.TIMESTAMP, mockFormat, "Time token"));
        tokenTypes.add(new TokenType("Mon", EncodingType.TEXT, mockFormat, "Month token"));
        tokenTypes.add(new TokenType("DD", EncodingType.INTEGER, mockFormat, "Day token"));
        tokenTypes.add(new TokenType("YYYY", EncodingType.INTEGER, mockFormat, "Year token"));
        tokenTypes.add(new TokenType("datetime", EncodingType.TIMESTAMP, mockFormat, "DateTime token"));
        tokenTypes.add(new TokenType("TZA", EncodingType.TEXT, mockFormat, "Timezone token"));
        tokenTypes.add(new TokenType("ip", EncodingType.IPV4, mockFormat, "IP address token"));
        tokenTypes.add(new TokenType("level", EncodingType.TEXT, mockFormat, "Log level token"));

        return tokenTypes;
    }

    @SuppressWarnings("SameParameterValue")
    private SubTokenType createMockSubTokenType(String name) {
        SubTokenBaseType mockBaseType = new SubTokenBaseType(
            "mockBase",
            EncodingType.TEXT,
            "M",
            String.class,
            "Mock base type",
            new char[] { 'a', 'b', 'c' }
        );
        return new SubTokenType(name, mockBaseType, "", "Mock sub token");
    }

    public void testParseMultiTokenFormat_SingleToken() {
        List<Object> result = PatternUtils.parseMultiTokenFormat("$time", tokenTypes, boundaryChars);

        assertEquals(1, result.size());
        assertThat(result.getFirst(), instanceOf(TokenType.class));
        assertEquals("time", ((TokenType) result.getFirst()).name());
    }

    public void testParseMultiTokenFormat_MultipleTokensWithSpaces() {
        List<Object> result = PatternUtils.parseMultiTokenFormat("$Mon $DD $YYYY", tokenTypes, boundaryChars);

        assertEquals(5, result.size());
        assertThat(result.get(0), instanceOf(TokenType.class));
        assertEquals("Mon", ((TokenType) result.get(0)).name());
        assertEquals(" ", result.get(1));
        assertThat(result.get(2), instanceOf(TokenType.class));
        assertEquals("DD", ((TokenType) result.get(2)).name());
        assertEquals(" ", result.get(3));
        assertThat(result.get(4), instanceOf(TokenType.class));
        assertEquals("YYYY", ((TokenType) result.get(4)).name());
    }

    public void testParseMultiTokenFormat_TokensWithCommaDelimiter() {
        List<Object> result = PatternUtils.parseMultiTokenFormat("$Mon, $DD $YYYY", tokenTypes, boundaryChars);

        assertEquals(5, result.size());
        assertThat(result.get(0), instanceOf(TokenType.class));
        assertEquals("Mon", ((TokenType) result.get(0)).name());
        assertEquals(", ", result.get(1));
        assertThat(result.get(2), instanceOf(TokenType.class));
        assertEquals("DD", ((TokenType) result.get(2)).name());
        assertEquals(" ", result.get(3));
        assertThat(result.get(4), instanceOf(TokenType.class));
        assertEquals("YYYY", ((TokenType) result.get(4)).name());
    }

    public void testParseMultiTokenFormat_IllegalLiteralAtStart() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PatternUtils.parseMultiTokenFormat("Date: $Mon $DD", tokenTypes, boundaryChars)
        );
        assertThat(
            e.getMessage(),
            containsString("Invalid format - only token delimiters and trimmed characters are allowed between tokens:")
        );
    }

    public void testParseMultiTokenFormat_IllegalLiteralAtEnd() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PatternUtils.parseMultiTokenFormat("$datetime $TZA (UTC)", tokenTypes, boundaryChars)
        );
        assertThat(
            e.getMessage(),
            containsString("Invalid format - only token delimiters and trimmed characters are allowed between tokens:")
        );
    }

    public void testParseMultiTokenFormat_LiteralTextBetweenWithoutDelimiter() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PatternUtils.parseMultiTokenFormat("$ip:$level", tokenTypes, boundaryChars)
        );
        assertThat(e.getMessage(), containsString("Token names must be separated by delimiters:"));
    }

    public void testParseMultiTokenFormat_OnlyLiteralText() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PatternUtils.parseMultiTokenFormat("No tokens here", tokenTypes, boundaryChars)
        );
        assertThat(
            e.getMessage(),
            containsString("Invalid format - only token delimiters and trimmed characters are allowed between tokens:")
        );
    }

    public void testParseMultiTokenFormat_EmptyString() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PatternUtils.parseMultiTokenFormat("", tokenTypes, boundaryChars)
        );
        assertEquals("Format string cannot be null or empty", e.getMessage());
    }

    public void testParseMultiTokenFormat_NullString() {
        @SuppressWarnings("DataFlowIssue")
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PatternUtils.parseMultiTokenFormat(null, tokenTypes, boundaryChars)
        );
        assertEquals("Format string cannot be null or empty", e.getMessage());
    }

    public void testParseMultiTokenFormat_UnknownTokenType() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PatternUtils.parseMultiTokenFormat("$unknown", tokenTypes, boundaryChars)
        );
        assertEquals("Unknown token type: unknown in format: $unknown", e.getMessage());
    }

    public void testParseMultiTokenFormat_InvalidTokenReference() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PatternUtils.parseMultiTokenFormat("$ ", tokenTypes, boundaryChars)
        );
        assertEquals("Token name cannot be empty in format: $ ", e.getMessage());
    }

    public void testParseMultiTokenFormat_DollarAtEnd() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PatternUtils.parseMultiTokenFormat("text$", tokenTypes, boundaryChars)
        );
        assertEquals("Invalid format - only token delimiters and trimmed characters are allowed between tokens: text$", e.getMessage());
    }

    public void testParseMultiTokenFormat_TokensBoundedByDifferentChars() {
        List<Object> result = PatternUtils.parseMultiTokenFormat("($time)[$level]", tokenTypes, boundaryChars);

        assertEquals(5, result.size());
        assertEquals("(", result.get(0));
        assertThat(result.get(1), instanceOf(TokenType.class));
        assertEquals("time", ((TokenType) result.get(1)).name());
        assertEquals(")[", result.get(2));
        assertThat(result.get(3), instanceOf(TokenType.class));
        assertEquals("level", ((TokenType) result.get(3)).name());
        assertEquals("]", result.get(4));
    }

    public void testParseMultiTokenFormat_WhitespaceHandling() {
        List<Object> result = PatternUtils.parseMultiTokenFormat("  $time\t$level", tokenTypes, boundaryChars);

        assertEquals(4, result.size());
        assertEquals("  ", result.get(0));
        assertThat(result.get(1), instanceOf(TokenType.class));
        assertEquals("time", ((TokenType) result.get(1)).name());
        assertEquals("\t", result.get(2));
        assertThat(result.get(3), instanceOf(TokenType.class));
        assertEquals("level", ((TokenType) result.get(3)).name());
    }
}
