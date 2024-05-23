/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.esql.parser.EsqlBaseParser.IdentifierContext;
import org.elasticsearch.xpack.esql.parser.EsqlBaseParser.IndexIdentifierContext;

import java.util.List;

import static org.elasticsearch.xpack.esql.core.parser.ParserUtils.visitList;

abstract class IdentifierBuilder extends AbstractBuilder {

    @Override
    public String visitIdentifier(IdentifierContext ctx) {
        return ctx == null ? null : unquoteIdentifier(ctx.QUOTED_IDENTIFIER(), ctx.UNQUOTED_IDENTIFIER());
    }

    @Override
    public String visitIndexIdentifier(IndexIdentifierContext ctx) {
        return ctx == null ? null : unquoteIdentifier(null, ctx.INDEX_UNQUOTED_IDENTIFIER());
    }

    protected static String unquoteIdentifier(TerminalNode quotedNode, TerminalNode unquotedNode) {
        String result;
        if (quotedNode != null) {
            result = unquoteIdString(quotedNode.getText());
        } else {
            result = unquotedNode.getText();
        }
        return result;
    }

    protected static String unquoteIdString(String quotedString) {
        return quotedString.substring(1, quotedString.length() - 1).replace("``", "`");
    }

    public String visitIndexIdentifiers(List<IndexIdentifierContext> ctx) {
        return Strings.collectionToDelimitedString(visitList(this, ctx, String.class), ",");
    }
}
