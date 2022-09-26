// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;

/**
 * This class provides an empty implementation of {@link EsqlBaseParserVisitor},
 * which can be extended to create a visitor which only needs to handle a subset
 * of the available methods.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public class EsqlBaseParserBaseVisitor<T> extends AbstractParseTreeVisitor<T> implements EsqlBaseParserVisitor<T> {
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitSingleStatement(EsqlBaseParser.SingleStatementContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitCompositeQuery(EsqlBaseParser.CompositeQueryContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitSingleCommandQuery(EsqlBaseParser.SingleCommandQueryContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitSourceCommand(EsqlBaseParser.SourceCommandContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitProcessingCommand(EsqlBaseParser.ProcessingCommandContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitWhereCommand(EsqlBaseParser.WhereCommandContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitLogicalNot(EsqlBaseParser.LogicalNotContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitBooleanDefault(EsqlBaseParser.BooleanDefaultContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitLogicalBinary(EsqlBaseParser.LogicalBinaryContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitValueExpressionDefault(EsqlBaseParser.ValueExpressionDefaultContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitComparison(EsqlBaseParser.ComparisonContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitOperatorExpressionDefault(EsqlBaseParser.OperatorExpressionDefaultContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitArithmeticBinary(EsqlBaseParser.ArithmeticBinaryContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitArithmeticUnary(EsqlBaseParser.ArithmeticUnaryContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitConstantDefault(EsqlBaseParser.ConstantDefaultContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitDereference(EsqlBaseParser.DereferenceContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitParenthesizedExpression(EsqlBaseParser.ParenthesizedExpressionContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitFunctionExpression(EsqlBaseParser.FunctionExpressionContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitRowCommand(EsqlBaseParser.RowCommandContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitFields(EsqlBaseParser.FieldsContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitField(EsqlBaseParser.FieldContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitFromCommand(EsqlBaseParser.FromCommandContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitEvalCommand(EsqlBaseParser.EvalCommandContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitStatsCommand(EsqlBaseParser.StatsCommandContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitSourceIdentifier(EsqlBaseParser.SourceIdentifierContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitQualifiedName(EsqlBaseParser.QualifiedNameContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitQualifiedNames(EsqlBaseParser.QualifiedNamesContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitIdentifier(EsqlBaseParser.IdentifierContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitNullLiteral(EsqlBaseParser.NullLiteralContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitNumericLiteral(EsqlBaseParser.NumericLiteralContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitBooleanLiteral(EsqlBaseParser.BooleanLiteralContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitStringLiteral(EsqlBaseParser.StringLiteralContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitLimitCommand(EsqlBaseParser.LimitCommandContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitSortCommand(EsqlBaseParser.SortCommandContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitOrderExpression(EsqlBaseParser.OrderExpressionContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitBooleanValue(EsqlBaseParser.BooleanValueContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitDecimalLiteral(EsqlBaseParser.DecimalLiteralContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitIntegerLiteral(EsqlBaseParser.IntegerLiteralContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitString(EsqlBaseParser.StringContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitComparisonOperator(EsqlBaseParser.ComparisonOperatorContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitExplainCommand(EsqlBaseParser.ExplainCommandContext ctx) { return visitChildren(ctx); }
  /**
   * {@inheritDoc}
   *
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   */
  @Override public T visitSubqueryExpression(EsqlBaseParser.SubqueryExpressionContext ctx) { return visitChildren(ctx); }
}
