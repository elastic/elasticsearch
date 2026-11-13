/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParserUtils;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParserBaseVisitor;

class AbstractPromqlBuilder extends PromqlBaseParserBaseVisitor<Object> {
    private final int startLine, startColumn;
    /**
     * The view whose definition this PromQL text belongs to, or {@code null} for the caller's query. Every {@link Source} is
     * tagged with it, as {@code ExpressionBuilder} does for ES|QL, so a data node reads its text from the view's query.
     */
    @Nullable
    private final String viewName;

    AbstractPromqlBuilder(int startLine, int startColumn, @Nullable String viewName) {
        this.startLine = startLine;
        this.startColumn = startColumn;
        this.viewName = viewName;
    }

    @Override
    public Object visit(ParseTree tree) {
        return ParserUtils.visit(super::visit, tree);
    }

    /**
     * Extracts the actual unescaped string (literal) value of a terminal node.
     */
    String string(TerminalNode node) {
        return node == null ? null : unquote(source(node));
    }

    static String unquoteString(Source source) {
        return source == null ? null : unquote(source);
    }

    static String unquote(Source source) {
        return PromqlParserUtils.unquote(source);
    }

    @Override
    public Object visitTerminal(TerminalNode node) {
        Source source = source(node);
        throw new ParsingException(source, "Does not know how to handle {}", source.text());
    }

    protected Source source(TerminalNode terminalNode) {
        return adjust(ParserUtils.source(terminalNode));
    }

    protected Source source(ParserRuleContext parserRuleContext) {
        return adjust(ParserUtils.source(parserRuleContext));
    }

    protected Source source(ParseTree parseTree) {
        return adjust(ParserUtils.source(parseTree));
    }

    protected Source source(Token token) {
        return adjust(ParserUtils.source(token));
    }

    protected Source source(Token start, Token stop) {
        return adjust(ParserUtils.source(start, stop));
    }

    private Source adjust(Source source) {
        Source adjusted = PromqlParserUtils.adjustSource(source, startLine, startColumn);
        return viewName == null ? adjusted : adjusted.withViewName(viewName);
    }

}
