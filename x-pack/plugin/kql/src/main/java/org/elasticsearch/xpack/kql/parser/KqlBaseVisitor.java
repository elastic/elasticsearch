// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.kql.parser;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link KqlBaseParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
interface KqlBaseVisitor<T> extends ParseTreeVisitor<T> {
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#topLevelQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitTopLevelQuery(KqlBaseParser.TopLevelQueryContext ctx);
    /**
     * Visit a parse tree produced by the {@code logicalNot}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitLogicalNot(KqlBaseParser.LogicalNotContext ctx);
    /**
     * Visit a parse tree produced by the {@code queryDefault}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitQueryDefault(KqlBaseParser.QueryDefaultContext ctx);
    /**
     * Visit a parse tree produced by the {@code logicalAnd}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitLogicalAnd(KqlBaseParser.LogicalAndContext ctx);
    /**
     * Visit a parse tree produced by the {@code logicalOr}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitLogicalOr(KqlBaseParser.LogicalOrContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#simpleQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitSimpleQuery(KqlBaseParser.SimpleQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#expression}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitExpression(KqlBaseParser.ExpressionContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#nestedQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitNestedQuery(KqlBaseParser.NestedQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#parenthesizedQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitParenthesizedQuery(KqlBaseParser.ParenthesizedQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#fieldRangeQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitFieldRangeQuery(KqlBaseParser.FieldRangeQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#fieldTermQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitFieldTermQuery(KqlBaseParser.FieldTermQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#fieldName}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitFieldName(KqlBaseParser.FieldNameContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#groupingExpr}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitGroupingExpr(KqlBaseParser.GroupingExprContext ctx);
    /**
     * Visit a parse tree produced by the {@code wildcard}
     * labeled alternative in {@link KqlBaseParser#literalExpression}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitWildcard(KqlBaseParser.WildcardContext ctx);
    /**
     * Visit a parse tree produced by the {@code quotedString}
     * labeled alternative in {@link KqlBaseParser#literalExpression}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitQuotedString(KqlBaseParser.QuotedStringContext ctx);
    /**
     * Visit a parse tree produced by the {@code defaultLiteralExpression}
     * labeled alternative in {@link KqlBaseParser#literalExpression}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitDefaultLiteralExpression(KqlBaseParser.DefaultLiteralExpressionContext ctx);
}
