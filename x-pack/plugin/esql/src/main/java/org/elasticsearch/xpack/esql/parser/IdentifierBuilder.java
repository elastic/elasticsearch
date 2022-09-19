/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;

import java.util.List;

import static org.elasticsearch.xpack.ql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.visitList;

public class IdentifierBuilder extends EsqlBaseParserBaseVisitor<Object> {
    @Override
    public String visitIdentifier(EsqlBaseParser.IdentifierContext ctx) {
        String identifier;
        if (ctx.QUOTED_IDENTIFIER() != null) {
            identifier = ctx.QUOTED_IDENTIFIER().getText();
            identifier = identifier.substring(1, identifier.length() - 1);
        } else {
            identifier = ctx.UNQUOTED_IDENTIFIER().getText();
        }
        return identifier;
    }

    @Override
    public UnresolvedAttribute visitQualifiedName(EsqlBaseParser.QualifiedNameContext ctx) {
        if (ctx == null) {
            return null;
        }

        return new UnresolvedAttribute(
            source(ctx),
            Strings.collectionToDelimitedString(visitList(this, ctx.identifier(), String.class), ".")
        );
    }

    @Override
    public List<UnresolvedAttribute> visitQualifiedNames(EsqlBaseParser.QualifiedNamesContext ctx) {
        return ctx.qualifiedName().stream().map(this::visitQualifiedName).toList();
    }

    @Override
    public String visitSourceIdentifier(EsqlBaseParser.SourceIdentifierContext ctx) {
        if (ctx.SRC_QUOTED_IDENTIFIER() != null) {
            String identifier = ctx.SRC_QUOTED_IDENTIFIER().getText();
            return identifier.substring(1, identifier.length() - 1);
        } else {
            return ctx.SRC_UNQUOTED_IDENTIFIER().getText();
        }
    }
}
