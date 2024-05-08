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
import org.elasticsearch.xpack.ql.parser.ParserUtils;

import java.util.List;

abstract class IdentifierBuilder extends AbstractBuilder {

    @Override
    public String visitIdentifier(IdentifierContext ctx) {
        return ctx == null ? null : unquoteIdentifier(ctx.QUOTED_IDENTIFIER(), ctx.UNQUOTED_IDENTIFIER());
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

    @Override
    public String visitFromSource(EsqlBaseParser.FromSourceContext ctx) {
        TerminalNode unquoted = ctx.FROM_UNQUOTED_SOURCE();
        return unquoted != null ? unquoted.getText() : unquoteString(visitTerminal(ctx.QUOTED_STRING()));
    }

    public String visitFromSource(List<EsqlBaseParser.FromSourceContext> ctx) {
        return Strings.collectionToDelimitedString(ParserUtils.visitList(this, ctx, String.class), ",");
    }
}
