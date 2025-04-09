/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.parser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.IdentifierContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.QualifiedNameContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.TableIdentifierContext;

import static org.elasticsearch.xpack.ql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.ql.parser.ParserUtils.visitList;

abstract class IdentifierBuilder extends AbstractBuilder {

    @Override
    public TableIdentifier visitTableIdentifier(TableIdentifierContext ctx) {
        if (ctx == null) {
            return null;
        }

        Source source = source(ctx);
        ParseTree tree = ctx.name != null ? ctx.name : ctx.TABLE_IDENTIFIER();
        String index = tree.getText();

        String cluster = visitIdentifier(ctx.catalog);
        String indexName = unquoteIdentifier(index);
        String selector = visitIdentifier(ctx.selector);

        if (cluster != null && selector != null) {
            throw new ParsingException(
                source,
                "Invalid index name [{}:{}::{}], Selectors are not yet supported on remote cluster patterns",
                cluster,
                indexName,
                selector
            );
        }

        if (selector != null) {
            try {
                IndexNameExpressionResolver.SelectorResolver.validateIndexSelectorString(indexName, selector);
            } catch (Exception e) {
                throw new ParsingException(source, e.getMessage());
            }
        }

        if (indexName.contains(IndexNameExpressionResolver.SelectorResolver.SELECTOR_SEPARATOR)) {
            if (selector != null) {
                throw new ParsingException(
                    source,
                    "Invalid index name [{}::{}], Invalid usage of :: separator, only one :: separator is allowed per expression",
                    indexName,
                    selector
                );
            }
            try {
                Tuple<String, String> split = IndexNameExpressionResolver.splitSelectorExpression(indexName);
                indexName = split.v1();
                selector = split.v2();
            } catch (Exception e) {
                throw new ParsingException(source, e.getMessage());
            }
            if (selector != null) {
                try {
                    IndexNameExpressionResolver.SelectorResolver.validateIndexSelectorString(indexName, selector);
                } catch (Exception e) {
                    throw new ParsingException(source, "Invalid index name [{}::{}], {}", indexName, selector, e.getMessage());
                }
            }
        }

        indexName = IndexNameExpressionResolver.combineSelectorExpression(indexName, selector);

        return new TableIdentifier(source, cluster, indexName);
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

        return Strings.collectionToDelimitedString(visitList(this, ctx.identifier(), String.class), ".");
    }

    private static String unquoteIdentifier(String identifier) {
        return identifier.replace("\"\"", "\"");
    }
}
