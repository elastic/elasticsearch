// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link EsqlBaseParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface EsqlBaseParserVisitor<T> extends ParseTreeVisitor<T> {
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#singleStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingleStatement(EsqlBaseParser.SingleStatementContext ctx);
  /**
   * Visit a parse tree produced by the {@code compositeQuery}
   * labeled alternative in {@link EsqlBaseParser#query}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCompositeQuery(EsqlBaseParser.CompositeQueryContext ctx);
  /**
   * Visit a parse tree produced by the {@code singleCommandQuery}
   * labeled alternative in {@link EsqlBaseParser#query}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingleCommandQuery(EsqlBaseParser.SingleCommandQueryContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#sourceCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSourceCommand(EsqlBaseParser.SourceCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#processingCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitProcessingCommand(EsqlBaseParser.ProcessingCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#whereCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitWhereCommand(EsqlBaseParser.WhereCommandContext ctx);
  /**
   * Visit a parse tree produced by the {@code logicalNot}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLogicalNot(EsqlBaseParser.LogicalNotContext ctx);
  /**
   * Visit a parse tree produced by the {@code booleanDefault}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBooleanDefault(EsqlBaseParser.BooleanDefaultContext ctx);
  /**
   * Visit a parse tree produced by the {@code logicalBinary}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLogicalBinary(EsqlBaseParser.LogicalBinaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code valueExpressionDefault}
   * labeled alternative in {@link EsqlBaseParser#valueExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitValueExpressionDefault(EsqlBaseParser.ValueExpressionDefaultContext ctx);
  /**
   * Visit a parse tree produced by the {@code comparison}
   * labeled alternative in {@link EsqlBaseParser#valueExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitComparison(EsqlBaseParser.ComparisonContext ctx);
  /**
   * Visit a parse tree produced by the {@code operatorExpressionDefault}
   * labeled alternative in {@link EsqlBaseParser#operatorExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitOperatorExpressionDefault(EsqlBaseParser.OperatorExpressionDefaultContext ctx);
  /**
   * Visit a parse tree produced by the {@code arithmeticBinary}
   * labeled alternative in {@link EsqlBaseParser#operatorExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArithmeticBinary(EsqlBaseParser.ArithmeticBinaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code arithmeticUnary}
   * labeled alternative in {@link EsqlBaseParser#operatorExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArithmeticUnary(EsqlBaseParser.ArithmeticUnaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code constantDefault}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitConstantDefault(EsqlBaseParser.ConstantDefaultContext ctx);
  /**
   * Visit a parse tree produced by the {@code dereference}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDereference(EsqlBaseParser.DereferenceContext ctx);
  /**
   * Visit a parse tree produced by the {@code parenthesizedExpression}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitParenthesizedExpression(EsqlBaseParser.ParenthesizedExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code functionExpression}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunctionExpression(EsqlBaseParser.FunctionExpressionContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#rowCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRowCommand(EsqlBaseParser.RowCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#fields}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFields(EsqlBaseParser.FieldsContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#field}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitField(EsqlBaseParser.FieldContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#fromCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFromCommand(EsqlBaseParser.FromCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#evalCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitEvalCommand(EsqlBaseParser.EvalCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#statsCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStatsCommand(EsqlBaseParser.StatsCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#sourceIdentifier}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSourceIdentifier(EsqlBaseParser.SourceIdentifierContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#qualifiedName}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQualifiedName(EsqlBaseParser.QualifiedNameContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#qualifiedNames}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQualifiedNames(EsqlBaseParser.QualifiedNamesContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#identifier}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIdentifier(EsqlBaseParser.IdentifierContext ctx);
  /**
   * Visit a parse tree produced by the {@code nullLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNullLiteral(EsqlBaseParser.NullLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code numericLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNumericLiteral(EsqlBaseParser.NumericLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code booleanLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBooleanLiteral(EsqlBaseParser.BooleanLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code stringLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStringLiteral(EsqlBaseParser.StringLiteralContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#limitCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLimitCommand(EsqlBaseParser.LimitCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#sortCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSortCommand(EsqlBaseParser.SortCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#orderExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitOrderExpression(EsqlBaseParser.OrderExpressionContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#booleanValue}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBooleanValue(EsqlBaseParser.BooleanValueContext ctx);
  /**
   * Visit a parse tree produced by the {@code decimalLiteral}
   * labeled alternative in {@link EsqlBaseParser#number}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDecimalLiteral(EsqlBaseParser.DecimalLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code integerLiteral}
   * labeled alternative in {@link EsqlBaseParser#number}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIntegerLiteral(EsqlBaseParser.IntegerLiteralContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#string}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitString(EsqlBaseParser.StringContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#comparisonOperator}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitComparisonOperator(EsqlBaseParser.ComparisonOperatorContext ctx);
}
