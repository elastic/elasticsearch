/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.kql.parser.KqlBaseParser.QUOTED_STRING;
import static org.elasticsearch.xpack.kql.parser.KqlBaseParser.UNQUOTED_LITERAL;
import static org.elasticsearch.xpack.kql.parser.KqlBaseParser.WILDCARD;
import static org.elasticsearch.xpack.kql.parser.ParserUtils.escapeLuceneQueryString;
import static org.elasticsearch.xpack.kql.parser.ParserUtils.extractText;
import static org.elasticsearch.xpack.kql.parser.ParserUtils.hasWildcard;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ParserUtilsTests extends ESTestCase {

    public void testExtractTestWithQuotedString() {
        // General case
        assertThat(extractText(parserRuleContext(quotedStringNode("foo"))), equalTo("foo"));

        // Empty string
        assertThat(extractText(parserRuleContext(quotedStringNode(""))), equalTo(""));

        // Whitespaces are preserved
        assertThat(extractText(parserRuleContext(quotedStringNode(" foo   bar  "))), equalTo(" foo   bar  "));

        // Quoted string does not need escaping for KQL keywords (and, or, ...)
        assertThat(extractText(parserRuleContext(quotedStringNode("not foo and bar or baz"))), equalTo("not foo and bar or baz"));

        // Quoted string does not need escaping for KQL special chars (e.g: '{', ':', ...)
        assertThat(extractText(parserRuleContext(quotedStringNode("foo*:'\u3000{(<bar>})"))), equalTo("foo*:'\u3000{(<bar>})"));

        // Escaped characters handling
        assertThat(extractText(parserRuleContext(quotedStringNode("\\\\"))), equalTo("\\"));
        assertThat(extractText(parserRuleContext(quotedStringNode("foo\\\\bar"))), equalTo("foo\\bar"));
        assertThat(extractText(parserRuleContext(quotedStringNode("foo\\\\"))), equalTo("foo\\"));
        assertThat(extractText(parserRuleContext(quotedStringNode("\\\\foo"))), equalTo("\\foo"));

        assertThat(extractText(parserRuleContext(quotedStringNode("\\\""))), equalTo("\""));
        assertThat(extractText(parserRuleContext(quotedStringNode("foo\\\"bar"))), equalTo("foo\"bar"));
        assertThat(extractText(parserRuleContext(quotedStringNode("foo\\\""))), equalTo("foo\""));
        assertThat(extractText(parserRuleContext(quotedStringNode("\\\"foo"))), equalTo("\"foo"));

        assertThat(extractText(parserRuleContext(quotedStringNode("\\t"))), equalTo("\t"));
        assertThat(extractText(parserRuleContext(quotedStringNode("foo\\tbar"))), equalTo("foo\tbar"));
        assertThat(extractText(parserRuleContext(quotedStringNode("foo\\t"))), equalTo("foo\t"));
        assertThat(extractText(parserRuleContext(quotedStringNode("\\tfoo"))), equalTo("\tfoo"));

        assertThat(extractText(parserRuleContext(quotedStringNode("\\n"))), equalTo("\n"));
        assertThat(extractText(parserRuleContext(quotedStringNode("foo\\nbar"))), equalTo("foo\nbar"));
        assertThat(extractText(parserRuleContext(quotedStringNode("foo\\n"))), equalTo("foo\n"));
        assertThat(extractText(parserRuleContext(quotedStringNode("\\nfoo"))), equalTo("\nfoo"));

        assertThat(extractText(parserRuleContext(quotedStringNode("\\r"))), equalTo("\r"));
        assertThat(extractText(parserRuleContext(quotedStringNode("foo\\rbar"))), equalTo("foo\rbar"));
        assertThat(extractText(parserRuleContext(quotedStringNode("foo\\r"))), equalTo("foo\r"));
        assertThat(extractText(parserRuleContext(quotedStringNode("\\rfoo"))), equalTo("\rfoo"));

        // Unicode characters handling (\u0041 is 'A')
        assertThat(extractText(parserRuleContext(quotedStringNode(format("\\u0041")))), equalTo("A"));
        assertThat(extractText(parserRuleContext(quotedStringNode(format("foo\\u0041bar")))), equalTo("fooAbar"));
        assertThat(extractText(parserRuleContext(quotedStringNode(format("foo\\u0041")))), equalTo("fooA"));
        assertThat(extractText(parserRuleContext(quotedStringNode(format("\\u0041foo")))), equalTo("Afoo"));
    }

    public void testExtractTestWithUnquotedLiteral() {
        // General case
        assertThat(extractText(parserRuleContext(literalNode("foo"))), equalTo("foo"));

        // KQL keywords unescaping
        assertThat(extractText(parserRuleContext(literalNode("\\not foo \\and bar \\or baz"))), equalTo("not foo and bar or baz"));
        assertThat(
            extractText(parserRuleContext(literalNode("\\\\not foo \\\\and bar \\\\or baz"))),
            equalTo("\\not foo \\and bar \\or baz")
        );

        // Escaped characters handling
        assertThat(extractText(parserRuleContext(literalNode("\\\\"))), equalTo("\\"));
        assertThat(extractText(parserRuleContext(literalNode("foo\\\\bar"))), equalTo("foo\\bar"));
        assertThat(extractText(parserRuleContext(literalNode("foo\\\\"))), equalTo("foo\\"));
        assertThat(extractText(parserRuleContext(literalNode("\\\\foo"))), equalTo("\\foo"));

        assertThat(extractText(parserRuleContext(literalNode("\\\""))), equalTo("\""));
        assertThat(extractText(parserRuleContext(literalNode("foo\\\"bar"))), equalTo("foo\"bar"));
        assertThat(extractText(parserRuleContext(literalNode("foo\\\""))), equalTo("foo\""));
        assertThat(extractText(parserRuleContext(literalNode("\\\"foo"))), equalTo("\"foo"));

        assertThat(extractText(parserRuleContext(literalNode("\\t"))), equalTo("\t"));
        assertThat(extractText(parserRuleContext(literalNode("foo\\tbar"))), equalTo("foo\tbar"));
        assertThat(extractText(parserRuleContext(literalNode("foo\\t"))), equalTo("foo\t"));
        assertThat(extractText(parserRuleContext(literalNode("\\tfoo"))), equalTo("\tfoo"));

        assertThat(extractText(parserRuleContext(literalNode("\\n"))), equalTo("\n"));
        assertThat(extractText(parserRuleContext(literalNode("foo\\nbar"))), equalTo("foo\nbar"));
        assertThat(extractText(parserRuleContext(literalNode("foo\\n"))), equalTo("foo\n"));
        assertThat(extractText(parserRuleContext(literalNode("\\nfoo"))), equalTo("\nfoo"));

        assertThat(extractText(parserRuleContext(literalNode("\\r"))), equalTo("\r"));
        assertThat(extractText(parserRuleContext(literalNode("foo\\rbar"))), equalTo("foo\rbar"));
        assertThat(extractText(parserRuleContext(literalNode("foo\\r"))), equalTo("foo\r"));
        assertThat(extractText(parserRuleContext(literalNode("\\rfoo"))), equalTo("\rfoo"));

        for (String escapedChar : List.of("(", ")", ":", "<", ">", "*", "{", "}")) {
            assertThat(extractText(parserRuleContext(literalNode(format("\\%s", escapedChar)))), equalTo(escapedChar));
            assertThat(
                extractText(parserRuleContext(literalNode(format("foo\\%sbar", escapedChar)))),
                equalTo(format("foo%sbar", escapedChar))
            );
            assertThat(extractText(parserRuleContext(literalNode(format("foo\\%s", escapedChar)))), equalTo(format("foo%s", escapedChar)));
            assertThat(extractText(parserRuleContext(literalNode(format("\\%sfoo", escapedChar)))), equalTo(format("%sfoo", escapedChar)));
        }

        // Unicode characters handling (\u0041 is 'A')
        assertThat(extractText(parserRuleContext(literalNode(format("\\u0041")))), equalTo("A"));
        assertThat(extractText(parserRuleContext(literalNode(format("foo\\u0041bar")))), equalTo("fooAbar"));
        assertThat(extractText(parserRuleContext(literalNode(format("foo\\u0041")))), equalTo("fooA"));
        assertThat(extractText(parserRuleContext(literalNode(format("\\u0041foo")))), equalTo("Afoo"));
    }

    public void testHasWildcard() {
        // No children
        assertFalse(hasWildcard(parserRuleContext(List.of())));

        // Lone wildcard
        assertTrue(hasWildcard(parserRuleContext(wildcardNode())));
        assertTrue(hasWildcard(parserRuleContext(randomTextNodeListWithNode(wildcardNode()))));

        // All children are literals
        assertFalse(hasWildcard(parserRuleContext(randomList(1, randomIntBetween(1, 100), ParserUtilsTests::randomLiteralNode))));

        // Quoted string
        assertFalse(hasWildcard(parserRuleContext(randomQuotedStringNode())));

        // Literal node containing the wildcard character
        assertTrue(hasWildcard(parserRuleContext(literalNode("f*oo"))));
        assertTrue(hasWildcard(parserRuleContext(literalNode("*foo"))));
        assertTrue(hasWildcard(parserRuleContext(literalNode("foo*"))));

        // Literal node containing the wildcard characters (escaped)
        assertFalse(hasWildcard(parserRuleContext(literalNode("f\\*oo"))));
        assertFalse(hasWildcard(parserRuleContext(literalNode("\\*foo"))));
        assertFalse(hasWildcard(parserRuleContext(literalNode("foo\\*"))));
    }

    public void testUnquotedLiteralInvalidUnicodeCodeParsing() {
        {
            // Invalid unicode digit (G)
            ParserRuleContext ctx = parserRuleContext(literalNode("\\u0G41"));
            KqlParsingException e = assertThrows(KqlParsingException.class, () -> extractText(ctx));
            assertThat(e.getMessage(), equalTo("line 0:3: Invalid unicode character code [0G41]"));
        }

        {
            // U+D800—U+DFFF can only be used as surrogate pairs and are not valid character codes.
            ParserRuleContext ctx = parserRuleContext(literalNode("\\uD900"));
            KqlParsingException e = assertThrows(KqlParsingException.class, () -> extractText(ctx));
            assertThat(e.getMessage(), equalTo("line 0:3: Invalid unicode character code, [D900] is a surrogate code"));
        }
    }

    public void testQuotedStringInvalidUnicodeCodeParsing() {
        {
            // Invalid unicode digit (G)
            ParserRuleContext ctx = parserRuleContext(quotedStringNode("\\u0G41"));
            KqlParsingException e = assertThrows(KqlParsingException.class, () -> extractText(ctx));
            assertThat(e.getMessage(), equalTo("line 0:4: Invalid unicode character code [0G41]"));
        }

        {
            // U+D800—U+DFFF can only be used as surrogate pairs and are not valid character codes.
            ParserRuleContext ctx = parserRuleContext(quotedStringNode("\\uD900"));
            KqlParsingException e = assertThrows(KqlParsingException.class, () -> extractText(ctx));
            assertThat(e.getMessage(), equalTo("line 0:4: Invalid unicode character code, [D900] is a surrogate code"));
        }
    }

    public void testEscapeLuceneQueryString() {
        // Quotes
        assertThat(escapeLuceneQueryString("\"The Pink Panther\"", randomBoolean()), equalTo("\\\"The Pink Panther\\\""));

        // Escape chars
        assertThat(escapeLuceneQueryString("The Pink \\ Panther", randomBoolean()), equalTo("The Pink \\\\ Panther"));

        // Field operations
        assertThat(escapeLuceneQueryString("title:Do it right", randomBoolean()), equalTo("title\\:Do it right"));
        assertThat(escapeLuceneQueryString("title:(pink panther)", randomBoolean()), equalTo("title\\:\\(pink panther\\)"));
        assertThat(escapeLuceneQueryString("title:-pink", randomBoolean()), equalTo("title\\:\\-pink"));
        assertThat(escapeLuceneQueryString("title:+pink", randomBoolean()), equalTo("title\\:\\+pink"));
        assertThat(escapeLuceneQueryString("title:pink~", randomBoolean()), equalTo("title\\:pink\\~"));
        assertThat(escapeLuceneQueryString("title:pink~3.5", randomBoolean()), equalTo("title\\:pink\\~3.5"));
        assertThat(escapeLuceneQueryString("title:pink panther^4", randomBoolean()), equalTo("title\\:pink panther\\^4"));
        assertThat(escapeLuceneQueryString("rating:[0 TO 5]", randomBoolean()), equalTo("rating\\:\\[0 TO 5\\]"));
        assertThat(escapeLuceneQueryString("rating:{0 TO 5}", randomBoolean()), equalTo("rating\\:\\{0 TO 5\\}"));

        // Boolean operators
        assertThat(escapeLuceneQueryString("foo || bar", randomBoolean()), equalTo("foo \\|\\| bar"));
        assertThat(escapeLuceneQueryString("foo && bar", randomBoolean()), equalTo("foo \\&\\& bar"));
        assertThat(escapeLuceneQueryString("!foo", randomBoolean()), equalTo("\\!foo"));

        // Wildcards:
        assertThat(escapeLuceneQueryString("te?t", randomBoolean()), equalTo("te\\?t"));
        assertThat(escapeLuceneQueryString("foo*", true), equalTo("foo*"));
        assertThat(escapeLuceneQueryString("*foo", true), equalTo("*foo"));
        assertThat(escapeLuceneQueryString("foo * bar", true), equalTo("foo * bar"));
        assertThat(escapeLuceneQueryString("foo*", false), equalTo("foo\\*"));
    }

    private static ParserRuleContext parserRuleContext(ParseTree child) {
        return parserRuleContext(List.of(child));
    }

    private static ParserRuleContext parserRuleContext(List<ParseTree> children) {
        ParserRuleContext ctx = new ParserRuleContext(null, randomInt());
        ctx.children = children;
        return ctx;
    }

    private static TerminalNode terminalNode(int type, String text) {
        Token symbol = mock(Token.class);
        when(symbol.getType()).thenReturn(type);
        when(symbol.getText()).thenReturn(text);
        when(symbol.getLine()).thenReturn(0);
        when(symbol.getCharPositionInLine()).thenReturn(0);
        return new TerminalNodeImpl(symbol);
    }

    private static List<ParseTree> randomTextNodeListWithNode(TerminalNode node) {
        List<ParseTree> nodes = new ArrayList<>(
            Stream.concat(Stream.generate(ParserUtilsTests::randomTextNode).limit(100), Stream.of(node)).toList()
        );
        Collections.shuffle(nodes, random());
        return nodes;
    }

    private static TerminalNode randomTextNode() {
        return switch (randomInt() % 3) {
            case 0 -> wildcardNode();
            case 1 -> randomQuotedStringNode();
            default -> randomLiteralNode();
        };
    }

    private static TerminalNode quotedStringNode(String quotedStringText) {
        return terminalNode(QUOTED_STRING, "\"" + quotedStringText + "\"");
    }

    private static TerminalNode randomQuotedStringNode() {
        return quotedStringNode(randomIdentifier());
    }

    private static TerminalNode literalNode(String literalText) {
        return terminalNode(UNQUOTED_LITERAL, literalText);
    }

    private static TerminalNode randomLiteralNode() {
        return terminalNode(UNQUOTED_LITERAL, randomIdentifier());
    }

    private static TerminalNode wildcardNode() {
        return terminalNode(WILDCARD, "*");
    }
}
