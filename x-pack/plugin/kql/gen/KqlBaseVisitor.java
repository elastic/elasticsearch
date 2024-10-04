// Generated from /Users/afoucret/git/elasticsearch/x-pack/plugin/kql/src/main/antlr/KqlBase.g4 by ANTLR 4.13.1

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
public interface KqlBaseVisitor<T> extends ParseTreeVisitor<T> {
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
	 * Visit a parse tree produced by {@link KqlBaseParser#rangeQueryValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRangeQueryValue(KqlBaseParser.RangeQueryValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link KqlBaseParser#termQueryValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTermQueryValue(KqlBaseParser.TermQueryValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link KqlBaseParser#groupingTermExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupingTermExpression(KqlBaseParser.GroupingTermExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link KqlBaseParser#unquotedLiteralExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnquotedLiteralExpression(KqlBaseParser.UnquotedLiteralExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link KqlBaseParser#quotedStringExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedStringExpression(KqlBaseParser.QuotedStringExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link KqlBaseParser#wildcardExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWildcardExpression(KqlBaseParser.WildcardExpressionContext ctx);
}