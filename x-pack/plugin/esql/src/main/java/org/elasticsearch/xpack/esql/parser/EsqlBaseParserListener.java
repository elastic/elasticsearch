// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link EsqlBaseParser}.
 */
public interface EsqlBaseParserListener extends ParseTreeListener {
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#singleStatement}.
   * @param ctx the parse tree
   */
  void enterSingleStatement(EsqlBaseParser.SingleStatementContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#singleStatement}.
   * @param ctx the parse tree
   */
  void exitSingleStatement(EsqlBaseParser.SingleStatementContext ctx);
  /**
   * Enter a parse tree produced by the {@code compositeQuery}
   * labeled alternative in {@link EsqlBaseParser#query}.
   * @param ctx the parse tree
   */
  void enterCompositeQuery(EsqlBaseParser.CompositeQueryContext ctx);
  /**
   * Exit a parse tree produced by the {@code compositeQuery}
   * labeled alternative in {@link EsqlBaseParser#query}.
   * @param ctx the parse tree
   */
  void exitCompositeQuery(EsqlBaseParser.CompositeQueryContext ctx);
  /**
   * Enter a parse tree produced by the {@code singleCommandQuery}
   * labeled alternative in {@link EsqlBaseParser#query}.
   * @param ctx the parse tree
   */
  void enterSingleCommandQuery(EsqlBaseParser.SingleCommandQueryContext ctx);
  /**
   * Exit a parse tree produced by the {@code singleCommandQuery}
   * labeled alternative in {@link EsqlBaseParser#query}.
   * @param ctx the parse tree
   */
  void exitSingleCommandQuery(EsqlBaseParser.SingleCommandQueryContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#sourceCommand}.
   * @param ctx the parse tree
   */
  void enterSourceCommand(EsqlBaseParser.SourceCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#sourceCommand}.
   * @param ctx the parse tree
   */
  void exitSourceCommand(EsqlBaseParser.SourceCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#processingCommand}.
   * @param ctx the parse tree
   */
  void enterProcessingCommand(EsqlBaseParser.ProcessingCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#processingCommand}.
   * @param ctx the parse tree
   */
  void exitProcessingCommand(EsqlBaseParser.ProcessingCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#whereCommand}.
   * @param ctx the parse tree
   */
  void enterWhereCommand(EsqlBaseParser.WhereCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#whereCommand}.
   * @param ctx the parse tree
   */
  void exitWhereCommand(EsqlBaseParser.WhereCommandContext ctx);
  /**
   * Enter a parse tree produced by the {@code logicalNot}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   */
  void enterLogicalNot(EsqlBaseParser.LogicalNotContext ctx);
  /**
   * Exit a parse tree produced by the {@code logicalNot}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   */
  void exitLogicalNot(EsqlBaseParser.LogicalNotContext ctx);
  /**
   * Enter a parse tree produced by the {@code booleanDefault}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   */
  void enterBooleanDefault(EsqlBaseParser.BooleanDefaultContext ctx);
  /**
   * Exit a parse tree produced by the {@code booleanDefault}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   */
  void exitBooleanDefault(EsqlBaseParser.BooleanDefaultContext ctx);
  /**
   * Enter a parse tree produced by the {@code logicalBinary}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   */
  void enterLogicalBinary(EsqlBaseParser.LogicalBinaryContext ctx);
  /**
   * Exit a parse tree produced by the {@code logicalBinary}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   */
  void exitLogicalBinary(EsqlBaseParser.LogicalBinaryContext ctx);
  /**
   * Enter a parse tree produced by the {@code valueExpressionDefault}
   * labeled alternative in {@link EsqlBaseParser#valueExpression}.
   * @param ctx the parse tree
   */
  void enterValueExpressionDefault(EsqlBaseParser.ValueExpressionDefaultContext ctx);
  /**
   * Exit a parse tree produced by the {@code valueExpressionDefault}
   * labeled alternative in {@link EsqlBaseParser#valueExpression}.
   * @param ctx the parse tree
   */
  void exitValueExpressionDefault(EsqlBaseParser.ValueExpressionDefaultContext ctx);
  /**
   * Enter a parse tree produced by the {@code comparison}
   * labeled alternative in {@link EsqlBaseParser#valueExpression}.
   * @param ctx the parse tree
   */
  void enterComparison(EsqlBaseParser.ComparisonContext ctx);
  /**
   * Exit a parse tree produced by the {@code comparison}
   * labeled alternative in {@link EsqlBaseParser#valueExpression}.
   * @param ctx the parse tree
   */
  void exitComparison(EsqlBaseParser.ComparisonContext ctx);
  /**
   * Enter a parse tree produced by the {@code operatorExpressionDefault}
   * labeled alternative in {@link EsqlBaseParser#operatorExpression}.
   * @param ctx the parse tree
   */
  void enterOperatorExpressionDefault(EsqlBaseParser.OperatorExpressionDefaultContext ctx);
  /**
   * Exit a parse tree produced by the {@code operatorExpressionDefault}
   * labeled alternative in {@link EsqlBaseParser#operatorExpression}.
   * @param ctx the parse tree
   */
  void exitOperatorExpressionDefault(EsqlBaseParser.OperatorExpressionDefaultContext ctx);
  /**
   * Enter a parse tree produced by the {@code arithmeticBinary}
   * labeled alternative in {@link EsqlBaseParser#operatorExpression}.
   * @param ctx the parse tree
   */
  void enterArithmeticBinary(EsqlBaseParser.ArithmeticBinaryContext ctx);
  /**
   * Exit a parse tree produced by the {@code arithmeticBinary}
   * labeled alternative in {@link EsqlBaseParser#operatorExpression}.
   * @param ctx the parse tree
   */
  void exitArithmeticBinary(EsqlBaseParser.ArithmeticBinaryContext ctx);
  /**
   * Enter a parse tree produced by the {@code arithmeticUnary}
   * labeled alternative in {@link EsqlBaseParser#operatorExpression}.
   * @param ctx the parse tree
   */
  void enterArithmeticUnary(EsqlBaseParser.ArithmeticUnaryContext ctx);
  /**
   * Exit a parse tree produced by the {@code arithmeticUnary}
   * labeled alternative in {@link EsqlBaseParser#operatorExpression}.
   * @param ctx the parse tree
   */
  void exitArithmeticUnary(EsqlBaseParser.ArithmeticUnaryContext ctx);
  /**
   * Enter a parse tree produced by the {@code constantDefault}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   */
  void enterConstantDefault(EsqlBaseParser.ConstantDefaultContext ctx);
  /**
   * Exit a parse tree produced by the {@code constantDefault}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   */
  void exitConstantDefault(EsqlBaseParser.ConstantDefaultContext ctx);
  /**
   * Enter a parse tree produced by the {@code dereference}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   */
  void enterDereference(EsqlBaseParser.DereferenceContext ctx);
  /**
   * Exit a parse tree produced by the {@code dereference}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   */
  void exitDereference(EsqlBaseParser.DereferenceContext ctx);
  /**
   * Enter a parse tree produced by the {@code parenthesizedExpression}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   */
  void enterParenthesizedExpression(EsqlBaseParser.ParenthesizedExpressionContext ctx);
  /**
   * Exit a parse tree produced by the {@code parenthesizedExpression}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   */
  void exitParenthesizedExpression(EsqlBaseParser.ParenthesizedExpressionContext ctx);
  /**
   * Enter a parse tree produced by the {@code functionExpression}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   */
  void enterFunctionExpression(EsqlBaseParser.FunctionExpressionContext ctx);
  /**
   * Exit a parse tree produced by the {@code functionExpression}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   */
  void exitFunctionExpression(EsqlBaseParser.FunctionExpressionContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#rowCommand}.
   * @param ctx the parse tree
   */
  void enterRowCommand(EsqlBaseParser.RowCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#rowCommand}.
   * @param ctx the parse tree
   */
  void exitRowCommand(EsqlBaseParser.RowCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#fields}.
   * @param ctx the parse tree
   */
  void enterFields(EsqlBaseParser.FieldsContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#fields}.
   * @param ctx the parse tree
   */
  void exitFields(EsqlBaseParser.FieldsContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#field}.
   * @param ctx the parse tree
   */
  void enterField(EsqlBaseParser.FieldContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#field}.
   * @param ctx the parse tree
   */
  void exitField(EsqlBaseParser.FieldContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#fromCommand}.
   * @param ctx the parse tree
   */
  void enterFromCommand(EsqlBaseParser.FromCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#fromCommand}.
   * @param ctx the parse tree
   */
  void exitFromCommand(EsqlBaseParser.FromCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#evalCommand}.
   * @param ctx the parse tree
   */
  void enterEvalCommand(EsqlBaseParser.EvalCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#evalCommand}.
   * @param ctx the parse tree
   */
  void exitEvalCommand(EsqlBaseParser.EvalCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#statsCommand}.
   * @param ctx the parse tree
   */
  void enterStatsCommand(EsqlBaseParser.StatsCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#statsCommand}.
   * @param ctx the parse tree
   */
  void exitStatsCommand(EsqlBaseParser.StatsCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#sourceIdentifier}.
   * @param ctx the parse tree
   */
  void enterSourceIdentifier(EsqlBaseParser.SourceIdentifierContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#sourceIdentifier}.
   * @param ctx the parse tree
   */
  void exitSourceIdentifier(EsqlBaseParser.SourceIdentifierContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#qualifiedName}.
   * @param ctx the parse tree
   */
  void enterQualifiedName(EsqlBaseParser.QualifiedNameContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#qualifiedName}.
   * @param ctx the parse tree
   */
  void exitQualifiedName(EsqlBaseParser.QualifiedNameContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#qualifiedNames}.
   * @param ctx the parse tree
   */
  void enterQualifiedNames(EsqlBaseParser.QualifiedNamesContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#qualifiedNames}.
   * @param ctx the parse tree
   */
  void exitQualifiedNames(EsqlBaseParser.QualifiedNamesContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#identifier}.
   * @param ctx the parse tree
   */
  void enterIdentifier(EsqlBaseParser.IdentifierContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#identifier}.
   * @param ctx the parse tree
   */
  void exitIdentifier(EsqlBaseParser.IdentifierContext ctx);
  /**
   * Enter a parse tree produced by the {@code nullLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void enterNullLiteral(EsqlBaseParser.NullLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code nullLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void exitNullLiteral(EsqlBaseParser.NullLiteralContext ctx);
  /**
   * Enter a parse tree produced by the {@code numericLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void enterNumericLiteral(EsqlBaseParser.NumericLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code numericLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void exitNumericLiteral(EsqlBaseParser.NumericLiteralContext ctx);
  /**
   * Enter a parse tree produced by the {@code booleanLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void enterBooleanLiteral(EsqlBaseParser.BooleanLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code booleanLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void exitBooleanLiteral(EsqlBaseParser.BooleanLiteralContext ctx);
  /**
   * Enter a parse tree produced by the {@code stringLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void enterStringLiteral(EsqlBaseParser.StringLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code stringLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void exitStringLiteral(EsqlBaseParser.StringLiteralContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#limitCommand}.
   * @param ctx the parse tree
   */
  void enterLimitCommand(EsqlBaseParser.LimitCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#limitCommand}.
   * @param ctx the parse tree
   */
  void exitLimitCommand(EsqlBaseParser.LimitCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#sortCommand}.
   * @param ctx the parse tree
   */
  void enterSortCommand(EsqlBaseParser.SortCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#sortCommand}.
   * @param ctx the parse tree
   */
  void exitSortCommand(EsqlBaseParser.SortCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#orderExpression}.
   * @param ctx the parse tree
   */
  void enterOrderExpression(EsqlBaseParser.OrderExpressionContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#orderExpression}.
   * @param ctx the parse tree
   */
  void exitOrderExpression(EsqlBaseParser.OrderExpressionContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#booleanValue}.
   * @param ctx the parse tree
   */
  void enterBooleanValue(EsqlBaseParser.BooleanValueContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#booleanValue}.
   * @param ctx the parse tree
   */
  void exitBooleanValue(EsqlBaseParser.BooleanValueContext ctx);
  /**
   * Enter a parse tree produced by the {@code decimalLiteral}
   * labeled alternative in {@link EsqlBaseParser#number}.
   * @param ctx the parse tree
   */
  void enterDecimalLiteral(EsqlBaseParser.DecimalLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code decimalLiteral}
   * labeled alternative in {@link EsqlBaseParser#number}.
   * @param ctx the parse tree
   */
  void exitDecimalLiteral(EsqlBaseParser.DecimalLiteralContext ctx);
  /**
   * Enter a parse tree produced by the {@code integerLiteral}
   * labeled alternative in {@link EsqlBaseParser#number}.
   * @param ctx the parse tree
   */
  void enterIntegerLiteral(EsqlBaseParser.IntegerLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code integerLiteral}
   * labeled alternative in {@link EsqlBaseParser#number}.
   * @param ctx the parse tree
   */
  void exitIntegerLiteral(EsqlBaseParser.IntegerLiteralContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#string}.
   * @param ctx the parse tree
   */
  void enterString(EsqlBaseParser.StringContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#string}.
   * @param ctx the parse tree
   */
  void exitString(EsqlBaseParser.StringContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#comparisonOperator}.
   * @param ctx the parse tree
   */
  void enterComparisonOperator(EsqlBaseParser.ComparisonOperatorContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#comparisonOperator}.
   * @param ctx the parse tree
   */
  void exitComparisonOperator(EsqlBaseParser.ComparisonOperatorContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#explainCommand}.
   * @param ctx the parse tree
   */
  void enterExplainCommand(EsqlBaseParser.ExplainCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#explainCommand}.
   * @param ctx the parse tree
   */
  void exitExplainCommand(EsqlBaseParser.ExplainCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#subqueryExpression}.
   * @param ctx the parse tree
   */
  void enterSubqueryExpression(EsqlBaseParser.SubqueryExpressionContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#subqueryExpression}.
   * @param ctx the parse tree
   */
  void exitSubqueryExpression(EsqlBaseParser.SubqueryExpressionContext ctx);
}
