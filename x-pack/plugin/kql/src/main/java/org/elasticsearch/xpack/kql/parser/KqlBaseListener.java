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
     * Enter a parse tree produced by the {@code booleanQuery}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     */
    void enterBooleanQuery(KqlBaseParser.BooleanQueryContext ctx);
    /**
     * Exit a parse tree produced by the {@code booleanQuery}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     */
    void exitBooleanQuery(KqlBaseParser.BooleanQueryContext ctx);
    /**
     * Enter a parse tree produced by the {@code defaultQuery}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     */
    void enterDefaultQuery(KqlBaseParser.DefaultQueryContext ctx);
    /**
     * Exit a parse tree produced by the {@code defaultQuery}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     */
    void exitDefaultQuery(KqlBaseParser.DefaultQueryContext ctx);
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
     * Enter a parse tree produced by {@link KqlBaseParser#notQuery}.
     * @param ctx the parse tree
     */
    void enterNotQuery(KqlBaseParser.NotQueryContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#notQuery}.
     * @param ctx the parse tree
     */
    void exitNotQuery(KqlBaseParser.NotQueryContext ctx);
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
     * Enter a parse tree produced by the {@code booleanNestedQuery}
     * labeled alternative in {@link KqlBaseParser#nestedSubQuery}.
     * @param ctx the parse tree
     */
    void enterBooleanNestedQuery(KqlBaseParser.BooleanNestedQueryContext ctx);
    /**
     * Exit a parse tree produced by the {@code booleanNestedQuery}
     * labeled alternative in {@link KqlBaseParser#nestedSubQuery}.
     * @param ctx the parse tree
     */
    void exitBooleanNestedQuery(KqlBaseParser.BooleanNestedQueryContext ctx);
    /**
     * Enter a parse tree produced by the {@code defaultNestedQuery}
     * labeled alternative in {@link KqlBaseParser#nestedSubQuery}.
     * @param ctx the parse tree
     */
    void enterDefaultNestedQuery(KqlBaseParser.DefaultNestedQueryContext ctx);
    /**
     * Exit a parse tree produced by the {@code defaultNestedQuery}
     * labeled alternative in {@link KqlBaseParser#nestedSubQuery}.
     * @param ctx the parse tree
     */
    void exitDefaultNestedQuery(KqlBaseParser.DefaultNestedQueryContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#nestedSimpleSubQuery}.
     * @param ctx the parse tree
     */
    void enterNestedSimpleSubQuery(KqlBaseParser.NestedSimpleSubQueryContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#nestedSimpleSubQuery}.
     * @param ctx the parse tree
     */
    void exitNestedSimpleSubQuery(KqlBaseParser.NestedSimpleSubQueryContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#nestedParenthesizedQuery}.
     * @param ctx the parse tree
     */
    void enterNestedParenthesizedQuery(KqlBaseParser.NestedParenthesizedQueryContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#nestedParenthesizedQuery}.
     * @param ctx the parse tree
     */
    void exitNestedParenthesizedQuery(KqlBaseParser.NestedParenthesizedQueryContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#matchAllQuery}.
     * @param ctx the parse tree
     */
    void enterMatchAllQuery(KqlBaseParser.MatchAllQueryContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#matchAllQuery}.
     * @param ctx the parse tree
     */
    void exitMatchAllQuery(KqlBaseParser.MatchAllQueryContext ctx);
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
     * Enter a parse tree produced by {@link KqlBaseParser#rangeQuery}.
     * @param ctx the parse tree
     */
    void enterRangeQuery(KqlBaseParser.RangeQueryContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#rangeQuery}.
     * @param ctx the parse tree
     */
    void exitRangeQuery(KqlBaseParser.RangeQueryContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#rangeQueryValue}.
     * @param ctx the parse tree
     */
    void enterRangeQueryValue(KqlBaseParser.RangeQueryValueContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#rangeQueryValue}.
     * @param ctx the parse tree
     */
    void exitRangeQueryValue(KqlBaseParser.RangeQueryValueContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#existsQuery}.
     * @param ctx the parse tree
     */
    void enterExistsQuery(KqlBaseParser.ExistsQueryContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#existsQuery}.
     * @param ctx the parse tree
     */
    void exitExistsQuery(KqlBaseParser.ExistsQueryContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#fieldQuery}.
     * @param ctx the parse tree
     */
    void enterFieldQuery(KqlBaseParser.FieldQueryContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#fieldQuery}.
     * @param ctx the parse tree
     */
    void exitFieldQuery(KqlBaseParser.FieldQueryContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#fieldLessQuery}.
     * @param ctx the parse tree
     */
    void enterFieldLessQuery(KqlBaseParser.FieldLessQueryContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#fieldLessQuery}.
     * @param ctx the parse tree
     */
    void exitFieldLessQuery(KqlBaseParser.FieldLessQueryContext ctx);
    /**
     * Enter a parse tree produced by {@link KqlBaseParser#fieldQueryValue}.
     * @param ctx the parse tree
     */
    void enterFieldQueryValue(KqlBaseParser.FieldQueryValueContext ctx);
    /**
     * Exit a parse tree produced by {@link KqlBaseParser#fieldQueryValue}.
     * @param ctx the parse tree
     */
    void exitFieldQueryValue(KqlBaseParser.FieldQueryValueContext ctx);
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
}
