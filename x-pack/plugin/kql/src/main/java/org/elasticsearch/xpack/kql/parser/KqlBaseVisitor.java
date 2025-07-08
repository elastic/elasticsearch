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
     * Visit a parse tree produced by the {@code booleanQuery}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitBooleanQuery(KqlBaseParser.BooleanQueryContext ctx);
    /**
     * Visit a parse tree produced by the {@code defaultQuery}
     * labeled alternative in {@link KqlBaseParser#query}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitDefaultQuery(KqlBaseParser.DefaultQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#simpleQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitSimpleQuery(KqlBaseParser.SimpleQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#notQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitNotQuery(KqlBaseParser.NotQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#nestedQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitNestedQuery(KqlBaseParser.NestedQueryContext ctx);
    /**
     * Visit a parse tree produced by the {@code booleanNestedQuery}
     * labeled alternative in {@link KqlBaseParser#nestedSubQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitBooleanNestedQuery(KqlBaseParser.BooleanNestedQueryContext ctx);
    /**
     * Visit a parse tree produced by the {@code defaultNestedQuery}
     * labeled alternative in {@link KqlBaseParser#nestedSubQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitDefaultNestedQuery(KqlBaseParser.DefaultNestedQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#nestedSimpleSubQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitNestedSimpleSubQuery(KqlBaseParser.NestedSimpleSubQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#nestedParenthesizedQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitNestedParenthesizedQuery(KqlBaseParser.NestedParenthesizedQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#matchAllQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitMatchAllQuery(KqlBaseParser.MatchAllQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#parenthesizedQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitParenthesizedQuery(KqlBaseParser.ParenthesizedQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#rangeQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitRangeQuery(KqlBaseParser.RangeQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#rangeQueryValue}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitRangeQueryValue(KqlBaseParser.RangeQueryValueContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#existsQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitExistsQuery(KqlBaseParser.ExistsQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#fieldQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitFieldQuery(KqlBaseParser.FieldQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#fieldLessQuery}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitFieldLessQuery(KqlBaseParser.FieldLessQueryContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#fieldQueryValue}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitFieldQueryValue(KqlBaseParser.FieldQueryValueContext ctx);
    /**
     * Visit a parse tree produced by {@link KqlBaseParser#fieldName}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    T visitFieldName(KqlBaseParser.FieldNameContext ctx);
}
