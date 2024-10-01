// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.kql.parser;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link KqlBaseParser}.
 */
interface KqlBaseListener extends ParseTreeListener {
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#topLevelQuery}.
     * @param ctx the parse tree
     */
    void enterTopLevelQuery(KqlBaseParser.TopLevelQueryContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#topLevelQuery}.
     * @param ctx the parse tree
     */
    void exitTopLevelQuery(KqlBaseParser.TopLevelQueryContext ctx);
    /**
     * Enter a parse tree produced by the {@code logicalNot}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     */
    void enterLogicalNot(KqlBaseParser.LogicalNotContext ctx);
    /**
     * Exit a parse tree produced by the {@code logicalNot}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     */
    void exitLogicalNot(KqlBaseParser.LogicalNotContext ctx);
    /**
     * Enter a parse tree produced by the {@code queryDefault}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     */
    void enterQueryDefault(KqlBaseParser.QueryDefaultContext ctx);
    /**
     * Exit a parse tree produced by the {@code queryDefault}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     */
    void exitQueryDefault(KqlBaseParser.QueryDefaultContext ctx);
    /**
     * Enter a parse tree produced by the {@code logicalAnd}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     */
    void enterLogicalAnd(KqlBaseParser.LogicalAndContext ctx);
    /**
     * Exit a parse tree produced by the {@code logicalAnd}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     */
    void exitLogicalAnd(KqlBaseParser.LogicalAndContext ctx);
    /**
     * Enter a parse tree produced by the {@code logicalOr}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     */
    void enterLogicalOr(KqlBaseParser.LogicalOrContext ctx);
    /**
     * Exit a parse tree produced by the {@code logicalOr}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     */
    void exitLogicalOr(KqlBaseParser.LogicalOrContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#simpleQuery}.
     * @param ctx the parse tree
     */
    void enterSimpleQuery(KqlBaseParser.SimpleQueryContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#simpleQuery}.
     * @param ctx the parse tree
     */
    void exitSimpleQuery(KqlBaseParser.SimpleQueryContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#expression}.
     * @param ctx the parse tree
     */
    void enterExpression(KqlBaseParser.ExpressionContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#expression}.
     * @param ctx the parse tree
     */
    void exitExpression(KqlBaseParser.ExpressionContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#nestedQuery}.
     * @param ctx the parse tree
     */
    void enterNestedQuery(KqlBaseParser.NestedQueryContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#nestedQuery}.
     * @param ctx the parse tree
     */
    void exitNestedQuery(KqlBaseParser.NestedQueryContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#parenthesizedQuery}.
     * @param ctx the parse tree
     */
    void enterParenthesizedQuery(KqlBaseParser.ParenthesizedQueryContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#parenthesizedQuery}.
     * @param ctx the parse tree
     */
    void exitParenthesizedQuery(KqlBaseParser.ParenthesizedQueryContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#fieldRangeQuery}.
     * @param ctx the parse tree
     */
    void enterFieldRangeQuery(KqlBaseParser.FieldRangeQueryContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#fieldRangeQuery}.
     * @param ctx the parse tree
     */
    void exitFieldRangeQuery(KqlBaseParser.FieldRangeQueryContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#fieldTermQuery}.
     * @param ctx the parse tree
     */
    void enterFieldTermQuery(KqlBaseParser.FieldTermQueryContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#fieldTermQuery}.
     * @param ctx the parse tree
     */
    void exitFieldTermQuery(KqlBaseParser.FieldTermQueryContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#fieldName}.
     * @param ctx the parse tree
     */
    void enterFieldName(KqlBaseParser.FieldNameContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#fieldName}.
     * @param ctx the parse tree
     */
    void exitFieldName(KqlBaseParser.FieldNameContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#groupingExpr}.
     * @param ctx the parse tree
     */
    void enterGroupingExpr(KqlBaseParser.GroupingExprContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#groupingExpr}.
     * @param ctx the parse tree
     */
    void exitGroupingExpr(KqlBaseParser.GroupingExprContext ctx);
    /**
     * Enter a parse tree produced by the {@code wildcard}
     * labeled alternative in {@link KqlBaseParser#literalExpression}.
     * @param ctx the parse tree
     */
    void enterWildcard(KqlBaseParser.WildcardContext ctx);
    /**
     * Exit a parse tree produced by the {@code wildcard}
     * labeled alternative in {@link KqlBaseParser#literalExpression}.
     * @param ctx the parse tree
     */
    void exitWildcard(KqlBaseParser.WildcardContext ctx);
    /**
     * Enter a parse tree produced by the {@code quotedString}
     * labeled alternative in {@link KqlBaseParser#literalExpression}.
     * @param ctx the parse tree
     */
    void enterQuotedString(KqlBaseParser.QuotedStringContext ctx);
    /**
     * Exit a parse tree produced by the {@code quotedString}
     * labeled alternative in {@link KqlBaseParser#literalExpression}.
     * @param ctx the parse tree
     */
    void exitQuotedString(KqlBaseParser.QuotedStringContext ctx);
    /**
     * Enter a parse tree produced by the {@code defaultLiteralExpression}
     * labeled alternative in {@link KqlBaseParser#literalExpression}.
     * @param ctx the parse tree
     */
    void enterDefaultLiteralExpression(KqlBaseParser.DefaultLiteralExpressionContext ctx);
    /**
     * Exit a parse tree produced by the {@code defaultLiteralExpression}
     * labeled alternative in {@link KqlBaseParser#literalExpression}.
     * @param ctx the parse tree
     */
    void exitDefaultLiteralExpression(KqlBaseParser.DefaultLiteralExpressionContext ctx);
}
