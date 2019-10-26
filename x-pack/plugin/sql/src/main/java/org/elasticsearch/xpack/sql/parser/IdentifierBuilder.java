/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.IdentifierContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.QualifiedNameContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.TableIdentifierContext;
import org.elasticsearch.xpack.sql.plan.TableIdentifier;
import org.elasticsearch.xpack.sql.tree.Source;

abstract class IdentifierBuilder extends AbstractBuilder {

    @Override
    public TableIdentifier visitTableIdentifier(TableIdentifierContext ctx) {
        if (ctx == null) {
            return null;
        }

        Source source = source(ctx);
        ParseTree tree = ctx.name != null ? ctx.name : ctx.TABLE_IDENTIFIER();
        String index = tree.getText();

        return new TableIdentifier(source, visitIdentifier(ctx.catalog), unquoteIdentifier(index));
    }

    @Override
    public String visitIdentifier(IdentifierContext ctx) {
        return ctx == null ? null : unquoteIdentifier(ctx.getText());
    }

    @Override
    public String visitQualifiedName(QualifiedNameContext ctx) {
        if (ctx == null) {
            return null;
        }

        return Strings.collectionToDelimitedString(visitList(ctx.identifier(), String.class), ".");
    }
    
    private static String unquoteIdentifier(String identifier) {
        return identifier.replace("\"\"", "\"");
    }
}
