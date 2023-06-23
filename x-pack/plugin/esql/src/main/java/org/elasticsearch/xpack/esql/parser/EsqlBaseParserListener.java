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
   * Enter a parse tree produced by the {@code regexExpression}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   */
  void enterRegexExpression(EsqlBaseParser.RegexExpressionContext ctx);
  /**
   * Exit a parse tree produced by the {@code regexExpression}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   */
  void exitRegexExpression(EsqlBaseParser.RegexExpressionContext ctx);
  /**
   * Enter a parse tree produced by the {@code logicalIn}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   */
  void enterLogicalIn(EsqlBaseParser.LogicalInContext ctx);
  /**
   * Exit a parse tree produced by the {@code logicalIn}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   */
  void exitLogicalIn(EsqlBaseParser.LogicalInContext ctx);
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
   * Enter a parse tree produced by {@link EsqlBaseParser#regexBooleanExpression}.
   * @param ctx the parse tree
   */
  void enterRegexBooleanExpression(EsqlBaseParser.RegexBooleanExpressionContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#regexBooleanExpression}.
   * @param ctx the parse tree
   */
  void exitRegexBooleanExpression(EsqlBaseParser.RegexBooleanExpressionContext ctx);
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
   * Enter a parse tree produced by {@link EsqlBaseParser#inlinestatsCommand}.
   * @param ctx the parse tree
   */
  void enterInlinestatsCommand(EsqlBaseParser.InlinestatsCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#inlinestatsCommand}.
   * @param ctx the parse tree
   */
  void exitInlinestatsCommand(EsqlBaseParser.InlinestatsCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#grouping}.
   * @param ctx the parse tree
   */
  void enterGrouping(EsqlBaseParser.GroupingContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#grouping}.
   * @param ctx the parse tree
   */
  void exitGrouping(EsqlBaseParser.GroupingContext ctx);
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
   * Enter a parse tree produced by the {@code qualifiedIntegerLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void enterQualifiedIntegerLiteral(EsqlBaseParser.QualifiedIntegerLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code qualifiedIntegerLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void exitQualifiedIntegerLiteral(EsqlBaseParser.QualifiedIntegerLiteralContext ctx);
  /**
   * Enter a parse tree produced by the {@code decimalLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void enterDecimalLiteral(EsqlBaseParser.DecimalLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code decimalLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void exitDecimalLiteral(EsqlBaseParser.DecimalLiteralContext ctx);
  /**
   * Enter a parse tree produced by the {@code integerLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void enterIntegerLiteral(EsqlBaseParser.IntegerLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code integerLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void exitIntegerLiteral(EsqlBaseParser.IntegerLiteralContext ctx);
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
   * Enter a parse tree produced by the {@code inputParam}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void enterInputParam(EsqlBaseParser.InputParamContext ctx);
  /**
   * Exit a parse tree produced by the {@code inputParam}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void exitInputParam(EsqlBaseParser.InputParamContext ctx);
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
   * Enter a parse tree produced by the {@code numericArrayLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void enterNumericArrayLiteral(EsqlBaseParser.NumericArrayLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code numericArrayLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void exitNumericArrayLiteral(EsqlBaseParser.NumericArrayLiteralContext ctx);
  /**
   * Enter a parse tree produced by the {@code booleanArrayLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void enterBooleanArrayLiteral(EsqlBaseParser.BooleanArrayLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code booleanArrayLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void exitBooleanArrayLiteral(EsqlBaseParser.BooleanArrayLiteralContext ctx);
  /**
   * Enter a parse tree produced by the {@code stringArrayLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void enterStringArrayLiteral(EsqlBaseParser.StringArrayLiteralContext ctx);
  /**
   * Exit a parse tree produced by the {@code stringArrayLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void exitStringArrayLiteral(EsqlBaseParser.StringArrayLiteralContext ctx);
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
   * Enter a parse tree produced by {@link EsqlBaseParser#keepCommand}.
   * @param ctx the parse tree
   */
  void enterKeepCommand(EsqlBaseParser.KeepCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#keepCommand}.
   * @param ctx the parse tree
   */
  void exitKeepCommand(EsqlBaseParser.KeepCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#dropCommand}.
   * @param ctx the parse tree
   */
  void enterDropCommand(EsqlBaseParser.DropCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#dropCommand}.
   * @param ctx the parse tree
   */
  void exitDropCommand(EsqlBaseParser.DropCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#renameCommand}.
   * @param ctx the parse tree
   */
  void enterRenameCommand(EsqlBaseParser.RenameCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#renameCommand}.
   * @param ctx the parse tree
   */
  void exitRenameCommand(EsqlBaseParser.RenameCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#renameClause}.
   * @param ctx the parse tree
   */
  void enterRenameClause(EsqlBaseParser.RenameClauseContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#renameClause}.
   * @param ctx the parse tree
   */
  void exitRenameClause(EsqlBaseParser.RenameClauseContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#dissectCommand}.
   * @param ctx the parse tree
   */
  void enterDissectCommand(EsqlBaseParser.DissectCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#dissectCommand}.
   * @param ctx the parse tree
   */
  void exitDissectCommand(EsqlBaseParser.DissectCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#grokCommand}.
   * @param ctx the parse tree
   */
  void enterGrokCommand(EsqlBaseParser.GrokCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#grokCommand}.
   * @param ctx the parse tree
   */
  void exitGrokCommand(EsqlBaseParser.GrokCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#mvExpandCommand}.
   * @param ctx the parse tree
   */
  void enterMvExpandCommand(EsqlBaseParser.MvExpandCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#mvExpandCommand}.
   * @param ctx the parse tree
   */
  void exitMvExpandCommand(EsqlBaseParser.MvExpandCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#commandOptions}.
   * @param ctx the parse tree
   */
  void enterCommandOptions(EsqlBaseParser.CommandOptionsContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#commandOptions}.
   * @param ctx the parse tree
   */
  void exitCommandOptions(EsqlBaseParser.CommandOptionsContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#commandOption}.
   * @param ctx the parse tree
   */
  void enterCommandOption(EsqlBaseParser.CommandOptionContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#commandOption}.
   * @param ctx the parse tree
   */
  void exitCommandOption(EsqlBaseParser.CommandOptionContext ctx);
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
   * Enter a parse tree produced by {@link EsqlBaseParser#numericValue}.
   * @param ctx the parse tree
   */
  void enterNumericValue(EsqlBaseParser.NumericValueContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#numericValue}.
   * @param ctx the parse tree
   */
  void exitNumericValue(EsqlBaseParser.NumericValueContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#decimalValue}.
   * @param ctx the parse tree
   */
  void enterDecimalValue(EsqlBaseParser.DecimalValueContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#decimalValue}.
   * @param ctx the parse tree
   */
  void exitDecimalValue(EsqlBaseParser.DecimalValueContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#integerValue}.
   * @param ctx the parse tree
   */
  void enterIntegerValue(EsqlBaseParser.IntegerValueContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#integerValue}.
   * @param ctx the parse tree
   */
  void exitIntegerValue(EsqlBaseParser.IntegerValueContext ctx);
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
  /**
   * Enter a parse tree produced by the {@code showInfo}
   * labeled alternative in {@link EsqlBaseParser#showCommand}.
   * @param ctx the parse tree
   */
  void enterShowInfo(EsqlBaseParser.ShowInfoContext ctx);
  /**
   * Exit a parse tree produced by the {@code showInfo}
   * labeled alternative in {@link EsqlBaseParser#showCommand}.
   * @param ctx the parse tree
   */
  void exitShowInfo(EsqlBaseParser.ShowInfoContext ctx);
  /**
   * Enter a parse tree produced by the {@code showFunctions}
   * labeled alternative in {@link EsqlBaseParser#showCommand}.
   * @param ctx the parse tree
   */
  void enterShowFunctions(EsqlBaseParser.ShowFunctionsContext ctx);
  /**
   * Exit a parse tree produced by the {@code showFunctions}
   * labeled alternative in {@link EsqlBaseParser#showCommand}.
   * @param ctx the parse tree
   */
  void exitShowFunctions(EsqlBaseParser.ShowFunctionsContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#enrichCommand}.
   * @param ctx the parse tree
   */
  void enterEnrichCommand(EsqlBaseParser.EnrichCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#enrichCommand}.
   * @param ctx the parse tree
   */
  void exitEnrichCommand(EsqlBaseParser.EnrichCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#enrichWithClause}.
   * @param ctx the parse tree
   */
  void enterEnrichWithClause(EsqlBaseParser.EnrichWithClauseContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#enrichWithClause}.
   * @param ctx the parse tree
   */
  void exitEnrichWithClause(EsqlBaseParser.EnrichWithClauseContext ctx);
}
