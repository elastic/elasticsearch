/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParserUtils;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParserBaseVisitor;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import static org.elasticsearch.xpack.esql.parser.ParserUtils.source;

class AbstractBuilder extends PromqlBaseParserBaseVisitor<Object> {
    @Override
    public Object visit(ParseTree tree) {
        return ParserUtils.visit(super::visit, tree);
    }

    /**
     * Extracts the actual unescaped string (literal) value of a terminal node.
     */
    static String string(TerminalNode node) {
        return node == null ? null : unquote(source(node));
    }

    static String unquoteString(Source source) {
        return source == null ? null : unquote(source);
    }

    static String unquote(Source source) {
        return ParsingUtils.unquote(source);
    }

    @Override
    public Object visitTerminal(TerminalNode node) {
        Source source = source(node);
        throw new ParsingException(source, "Does not know how to handle {}", source.text());
    }
}
