// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link PromqlBaseParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface PromqlBaseParserVisitor<T> extends ParseTreeVisitor<T> {
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#singleStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingleStatement(PromqlBaseParser.SingleStatementContext ctx);
  /**
   * Visit a parse tree produced by the {@code valueExpression}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitValueExpression(PromqlBaseParser.ValueExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code subquery}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSubquery(PromqlBaseParser.SubqueryContext ctx);
  /**
   * Visit a parse tree produced by the {@code parenthesized}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitParenthesized(PromqlBaseParser.ParenthesizedContext ctx);
  /**
   * Visit a parse tree produced by the {@code arithmeticBinary}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArithmeticBinary(PromqlBaseParser.ArithmeticBinaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code arithmeticUnary}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArithmeticUnary(PromqlBaseParser.ArithmeticUnaryContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#subqueryResolution}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSubqueryResolution(PromqlBaseParser.SubqueryResolutionContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#value}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitValue(PromqlBaseParser.ValueContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#function}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunction(PromqlBaseParser.FunctionContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#functionParams}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunctionParams(PromqlBaseParser.FunctionParamsContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#grouping}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitGrouping(PromqlBaseParser.GroupingContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#selector}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSelector(PromqlBaseParser.SelectorContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#seriesMatcher}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSeriesMatcher(PromqlBaseParser.SeriesMatcherContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#modifier}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitModifier(PromqlBaseParser.ModifierContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#labelList}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLabelList(PromqlBaseParser.LabelListContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#labels}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLabels(PromqlBaseParser.LabelsContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#label}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLabel(PromqlBaseParser.LabelContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#labelName}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLabelName(PromqlBaseParser.LabelNameContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#identifier}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIdentifier(PromqlBaseParser.IdentifierContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#evaluation}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitEvaluation(PromqlBaseParser.EvaluationContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#offset}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitOffset(PromqlBaseParser.OffsetContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#duration}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDuration(PromqlBaseParser.DurationContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#at}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAt(PromqlBaseParser.AtContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitConstant(PromqlBaseParser.ConstantContext ctx);
  /**
   * Visit a parse tree produced by the {@code decimalLiteral}
   * labeled alternative in {@link PromqlBaseParser#number}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDecimalLiteral(PromqlBaseParser.DecimalLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code integerLiteral}
   * labeled alternative in {@link PromqlBaseParser#number}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIntegerLiteral(PromqlBaseParser.IntegerLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code hexLiteral}
   * labeled alternative in {@link PromqlBaseParser#number}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitHexLiteral(PromqlBaseParser.HexLiteralContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#string}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitString(PromqlBaseParser.StringContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#timeValue}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTimeValue(PromqlBaseParser.TimeValueContext ctx);
  /**
   * Visit a parse tree produced by {@link PromqlBaseParser#nonReserved}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNonReserved(PromqlBaseParser.NonReservedContext ctx);
}
