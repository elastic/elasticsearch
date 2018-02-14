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
import org.elasticsearch.xpack.sql.tree.Location;

abstract class IdentifierBuilder extends AbstractBuilder {

    @Override
    public TableIdentifier visitTableIdentifier(TableIdentifierContext ctx) {
        Location source = source(ctx);
        ParseTree tree = ctx.name != null ? ctx.name : ctx.TABLE_IDENTIFIER();
        String index = tree.getText();

        validateIndex(index, source);
        return new TableIdentifier(source, visitIdentifier(ctx.catalog), index);
    }

    // see https://github.com/elastic/elasticsearch/issues/6736
    static void validateIndex(String index, Location source) {
        for (int i = 0; i < index.length(); i++) {
            char c = index.charAt(i);
            if (Character.isUpperCase(c)) {
                throw new ParsingException(source, "Invalid index name (needs to be lowercase) {}", index);
            }
            if (c == '\\' || c == '/' || c == '<' || c == '>' || c == '|' || c == ',' || c == ' ') {
                throw new ParsingException(source, "Invalid index name (illegal character {}) {}", c, index);
            }
        }
    }

    @Override
    public String visitIdentifier(IdentifierContext ctx) {
        return ctx == null ? null : ctx.getText();
    }

    @Override
    public String visitQualifiedName(QualifiedNameContext ctx) {
        if (ctx == null) {
            return null;
        }

        return Strings.collectionToDelimitedString(visitList(ctx.identifier(), String.class), ".");
    }
}
