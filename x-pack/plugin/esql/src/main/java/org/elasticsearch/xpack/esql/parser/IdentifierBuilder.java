/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.common.Strings;

import java.util.List;

import static org.elasticsearch.xpack.ql.parser.ParserUtils.visitList;

abstract class IdentifierBuilder extends AbstractBuilder {

    @Override
    public String visitIdentifier(EsqlBaseParser.IdentifierContext ctx) {
        return unquoteIdentifier(ctx.QUOTED_IDENTIFIER(), ctx.UNQUOTED_IDENTIFIER());
    }

    @Override
    public String visitSourceIdentifier(EsqlBaseParser.SourceIdentifierContext ctx) {
        return unquoteIdentifier(ctx.SRC_QUOTED_IDENTIFIER(), ctx.SRC_UNQUOTED_IDENTIFIER());
    }

    private static String unquoteIdentifier(TerminalNode quotedNode, TerminalNode unquotedNode) {
        String result;
        if (quotedNode != null) {
            String identifier = quotedNode.getText();
            result = identifier.substring(1, identifier.length() - 1);
        } else {
            result = unquotedNode.getText();
        }
        return result;
    }

    public String visitSourceIdentifiers(List<EsqlBaseParser.SourceIdentifierContext> ctx) {
        return Strings.collectionToDelimitedString(visitList(this, ctx, String.class), ",");
    }
}
