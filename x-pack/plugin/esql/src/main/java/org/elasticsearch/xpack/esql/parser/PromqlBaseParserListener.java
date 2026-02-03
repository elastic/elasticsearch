// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link PromqlBaseParser}.
 */
public interface PromqlBaseParserListener extends ParseTreeListener {
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#singleStatement}.
   * @param ctx the parse tree
   */
  void enterSingleStatement(PromqlBaseParser.SingleStatementContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#singleStatement}.
   * @param ctx the parse tree
   */
  void exitSingleStatement(PromqlBaseParser.SingleStatementContext ctx);
  /**
   * Enter a parse tree produced by the {@code valueExpression}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   */
  void enterValueExpression(PromqlBaseParser.ValueExpressionContext ctx);
  /**
   * Exit a parse tree produced by the {@code valueExpression}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   */
  void exitValueExpression(PromqlBaseParser.ValueExpressionContext ctx);
  /**
   * Enter a parse tree produced by the {@code subquery}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   */
  void enterSubquery(PromqlBaseParser.SubqueryContext ctx);
  /**
   * Exit a parse tree produced by the {@code subquery}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   */
  void exitSubquery(PromqlBaseParser.SubqueryContext ctx);
  /**
   * Enter a parse tree produced by the {@code parenthesized}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   */
  void enterParenthesized(PromqlBaseParser.ParenthesizedContext ctx);
  /**
   * Exit a parse tree produced by the {@code parenthesized}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   */
  void exitParenthesized(PromqlBaseParser.ParenthesizedContext ctx);
  /**
   * Enter a parse tree produced by the {@code arithmeticBinary}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   */
  void enterArithmeticBinary(PromqlBaseParser.ArithmeticBinaryContext ctx);
  /**
   * Exit a parse tree produced by the {@code arithmeticBinary}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   */
  void exitArithmeticBinary(PromqlBaseParser.ArithmeticBinaryContext ctx);
  /**
   * Enter a parse tree produced by the {@code arithmeticUnary}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   */
  void enterArithmeticUnary(PromqlBaseParser.ArithmeticUnaryContext ctx);
  /**
   * Exit a parse tree produced by the {@code arithmeticUnary}
   * labeled alternative in {@link PromqlBaseParser#expression}.
   * @param ctx the parse tree
   */
  void exitArithmeticUnary(PromqlBaseParser.ArithmeticUnaryContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#subqueryResolution}.
   * @param ctx the parse tree
   */
  void enterSubqueryResolution(PromqlBaseParser.SubqueryResolutionContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#subqueryResolution}.
   * @param ctx the parse tree
   */
  void exitSubqueryResolution(PromqlBaseParser.SubqueryResolutionContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#value}.
   * @param ctx the parse tree
   */
  void enterValue(PromqlBaseParser.ValueContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#value}.
   * @param ctx the parse tree
   */
  void exitValue(PromqlBaseParser.ValueContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#function}.
   * @param ctx the parse tree
   */
  void enterFunction(PromqlBaseParser.FunctionContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#function}.
   * @param ctx the parse tree
   */
  void exitFunction(PromqlBaseParser.FunctionContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#functionParams}.
   * @param ctx the parse tree
   */
  void enterFunctionParams(PromqlBaseParser.FunctionParamsContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#functionParams}.
   * @param ctx the parse tree
   */
  void exitFunctionParams(PromqlBaseParser.FunctionParamsContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#grouping}.
   * @param ctx the parse tree
   */
  void enterGrouping(PromqlBaseParser.GroupingContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#grouping}.
   * @param ctx the parse tree
   */
  void exitGrouping(PromqlBaseParser.GroupingContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#selector}.
   * @param ctx the parse tree
   */
  void enterSelector(PromqlBaseParser.SelectorContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#selector}.
   * @param ctx the parse tree
   */
  void exitSelector(PromqlBaseParser.SelectorContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#seriesMatcher}.
   * @param ctx the parse tree
   */
  void enterSeriesMatcher(PromqlBaseParser.SeriesMatcherContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#seriesMatcher}.
   * @param ctx the parse tree
   */
  void exitSeriesMatcher(PromqlBaseParser.SeriesMatcherContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#modifier}.
   * @param ctx the parse tree
   */
  void enterModifier(PromqlBaseParser.ModifierContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#modifier}.
   * @param ctx the parse tree
   */
  void exitModifier(PromqlBaseParser.ModifierContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#labelList}.
   * @param ctx the parse tree
   */
  void enterLabelList(PromqlBaseParser.LabelListContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#labelList}.
   * @param ctx the parse tree
   */
  void exitLabelList(PromqlBaseParser.LabelListContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#labels}.
   * @param ctx the parse tree
   */
  void enterLabels(PromqlBaseParser.LabelsContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#labels}.
   * @param ctx the parse tree
   */
  void exitLabels(PromqlBaseParser.LabelsContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#label}.
   * @param ctx the parse tree
   */
  void enterLabel(PromqlBaseParser.LabelContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#label}.
   * @param ctx the parse tree
   */
  void exitLabel(PromqlBaseParser.LabelContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#labelName}.
   * @param ctx the parse tree
   */
  void enterLabelName(PromqlBaseParser.LabelNameContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#labelName}.
   * @param ctx the parse tree
   */
  void exitLabelName(PromqlBaseParser.LabelNameContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#identifier}.
   * @param ctx the parse tree
   */
  void enterIdentifier(PromqlBaseParser.IdentifierContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#identifier}.
   * @param ctx the parse tree
   */
  void exitIdentifier(PromqlBaseParser.IdentifierContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#evaluation}.
   * @param ctx the parse tree
   */
  void enterEvaluation(PromqlBaseParser.EvaluationContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#evaluation}.
   * @param ctx the parse tree
   */
  void exitEvaluation(PromqlBaseParser.EvaluationContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#offset}.
   * @param ctx the parse tree
   */
  void enterOffset(PromqlBaseParser.OffsetContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#offset}.
   * @param ctx the parse tree
   */
  void exitOffset(PromqlBaseParser.OffsetContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#duration}.
   * @param ctx the parse tree
   */
  void enterDuration(PromqlBaseParser.DurationContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#duration}.
   * @param ctx the parse tree
   */
  void exitDuration(PromqlBaseParser.DurationContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#at}.
   * @param ctx the parse tree
   */
  void enterAt(PromqlBaseParser.AtContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#at}.
   * @param ctx the parse tree
   */
  void exitAt(PromqlBaseParser.AtContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void enterConstant(PromqlBaseParser.ConstantContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void exitConstant(PromqlBaseParser.ConstantContext ctx);
  /**
   * Enter a parse tree produced by the {@code decimalLiteral}
   * labeled alternative in {@link PromqlBaseParser#number}.
   * @param ctx the parse tree
   */
  void enterDecimalLiteral(PromqlBaseParser.DecimalLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code decimalLiteral}
   * labeled alternative in {@link PromqlBaseParser#number}.
   * @param ctx the parse tree
   */
  void exitDecimalLiteral(PromqlBaseParser.DecimalLiteralContext ctx);
  /**
   * Enter a parse tree produced by the {@code integerLiteral}
   * labeled alternative in {@link PromqlBaseParser#number}.
   * @param ctx the parse tree
   */
  void enterIntegerLiteral(PromqlBaseParser.IntegerLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code integerLiteral}
   * labeled alternative in {@link PromqlBaseParser#number}.
   * @param ctx the parse tree
   */
  void exitIntegerLiteral(PromqlBaseParser.IntegerLiteralContext ctx);
  /**
   * Enter a parse tree produced by the {@code hexLiteral}
   * labeled alternative in {@link PromqlBaseParser#number}.
   * @param ctx the parse tree
   */
  void enterHexLiteral(PromqlBaseParser.HexLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code hexLiteral}
   * labeled alternative in {@link PromqlBaseParser#number}.
   * @param ctx the parse tree
   */
  void exitHexLiteral(PromqlBaseParser.HexLiteralContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#string}.
   * @param ctx the parse tree
   */
  void enterString(PromqlBaseParser.StringContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#string}.
   * @param ctx the parse tree
   */
  void exitString(PromqlBaseParser.StringContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#timeValue}.
   * @param ctx the parse tree
   */
  void enterTimeValue(PromqlBaseParser.TimeValueContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#timeValue}.
   * @param ctx the parse tree
   */
  void exitTimeValue(PromqlBaseParser.TimeValueContext ctx);
  /**
   * Enter a parse tree produced by {@link PromqlBaseParser#nonReserved}.
   * @param ctx the parse tree
   */
  void enterNonReserved(PromqlBaseParser.NonReservedContext ctx);
  /**
   * Exit a parse tree produced by {@link PromqlBaseParser#nonReserved}.
   * @param ctx the parse tree
   */
  void exitNonReserved(PromqlBaseParser.NonReservedContext ctx);
}
