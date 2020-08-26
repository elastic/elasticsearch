// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.eql.parser;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link EqlBaseParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
interface EqlBaseVisitor<T> extends ParseTreeVisitor<T> {
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#singleStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingleStatement(EqlBaseParser.SingleStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#singleExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingleExpression(EqlBaseParser.SingleExpressionContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStatement(EqlBaseParser.StatementContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#query}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQuery(EqlBaseParser.QueryContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#sequenceParams}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSequenceParams(EqlBaseParser.SequenceParamsContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#sequence}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSequence(EqlBaseParser.SequenceContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#join}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitJoin(EqlBaseParser.JoinContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#pipe}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPipe(EqlBaseParser.PipeContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#joinKeys}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitJoinKeys(EqlBaseParser.JoinKeysContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#joinTerm}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitJoinTerm(EqlBaseParser.JoinTermContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#sequenceTerm}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSequenceTerm(EqlBaseParser.SequenceTermContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#subquery}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSubquery(EqlBaseParser.SubqueryContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#eventQuery}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitEventQuery(EqlBaseParser.EventQueryContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#eventFilter}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitEventFilter(EqlBaseParser.EventFilterContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExpression(EqlBaseParser.ExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code logicalNot}
   * labeled alternative in {@link EqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLogicalNot(EqlBaseParser.LogicalNotContext ctx);
  /**
   * Visit a parse tree produced by the {@code booleanDefault}
   * labeled alternative in {@link EqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBooleanDefault(EqlBaseParser.BooleanDefaultContext ctx);
  /**
   * Visit a parse tree produced by the {@code processCheck}
   * labeled alternative in {@link EqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitProcessCheck(EqlBaseParser.ProcessCheckContext ctx);
  /**
   * Visit a parse tree produced by the {@code logicalBinary}
   * labeled alternative in {@link EqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLogicalBinary(EqlBaseParser.LogicalBinaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code valueExpressionDefault}
   * labeled alternative in {@link EqlBaseParser#valueExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitValueExpressionDefault(EqlBaseParser.ValueExpressionDefaultContext ctx);
  /**
   * Visit a parse tree produced by the {@code comparison}
   * labeled alternative in {@link EqlBaseParser#valueExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitComparison(EqlBaseParser.ComparisonContext ctx);
  /**
   * Visit a parse tree produced by the {@code arithmeticBinary}
   * labeled alternative in {@link EqlBaseParser#valueExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArithmeticBinary(EqlBaseParser.ArithmeticBinaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code arithmeticUnary}
   * labeled alternative in {@link EqlBaseParser#valueExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArithmeticUnary(EqlBaseParser.ArithmeticUnaryContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#predicate}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPredicate(EqlBaseParser.PredicateContext ctx);
  /**
   * Visit a parse tree produced by the {@code constantDefault}
   * labeled alternative in {@link EqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitConstantDefault(EqlBaseParser.ConstantDefaultContext ctx);
  /**
   * Visit a parse tree produced by the {@code function}
   * labeled alternative in {@link EqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunction(EqlBaseParser.FunctionContext ctx);
  /**
   * Visit a parse tree produced by the {@code dereference}
   * labeled alternative in {@link EqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDereference(EqlBaseParser.DereferenceContext ctx);
  /**
   * Visit a parse tree produced by the {@code parenthesizedExpression}
   * labeled alternative in {@link EqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitParenthesizedExpression(EqlBaseParser.ParenthesizedExpressionContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#functionExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunctionExpression(EqlBaseParser.FunctionExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code nullLiteral}
   * labeled alternative in {@link EqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNullLiteral(EqlBaseParser.NullLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code numericLiteral}
   * labeled alternative in {@link EqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNumericLiteral(EqlBaseParser.NumericLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code booleanLiteral}
   * labeled alternative in {@link EqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBooleanLiteral(EqlBaseParser.BooleanLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code stringLiteral}
   * labeled alternative in {@link EqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStringLiteral(EqlBaseParser.StringLiteralContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#comparisonOperator}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitComparisonOperator(EqlBaseParser.ComparisonOperatorContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#booleanValue}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBooleanValue(EqlBaseParser.BooleanValueContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#qualifiedName}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQualifiedName(EqlBaseParser.QualifiedNameContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#identifier}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIdentifier(EqlBaseParser.IdentifierContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#timeUnit}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTimeUnit(EqlBaseParser.TimeUnitContext ctx);
  /**
   * Visit a parse tree produced by the {@code decimalLiteral}
   * labeled alternative in {@link EqlBaseParser#number}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDecimalLiteral(EqlBaseParser.DecimalLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code integerLiteral}
   * labeled alternative in {@link EqlBaseParser#number}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIntegerLiteral(EqlBaseParser.IntegerLiteralContext ctx);
  /**
   * Visit a parse tree produced by {@link EqlBaseParser#string}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitString(EqlBaseParser.StringContext ctx);
}
