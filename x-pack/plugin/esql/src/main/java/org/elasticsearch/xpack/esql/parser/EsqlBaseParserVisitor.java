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
 * by {@link EsqlBaseParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface EsqlBaseParserVisitor<T> extends ParseTreeVisitor<T> {
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#statements}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStatements(EsqlBaseParser.StatementsContext ctx);
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
   * Visit a parse tree produced by the {@code toDataType}
   * labeled alternative in {@link EsqlBaseParser#dataType}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitToDataType(EsqlBaseParser.ToDataTypeContext ctx);
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
   * Visit a parse tree produced by {@link EsqlBaseParser#timeSeriesCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTimeSeriesCommand(EsqlBaseParser.TimeSeriesCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#indexPatternAndMetadataFields}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIndexPatternAndMetadataFields(EsqlBaseParser.IndexPatternAndMetadataFieldsContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#indexPatternOrSubquery}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIndexPatternOrSubquery(EsqlBaseParser.IndexPatternOrSubqueryContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#subquery}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSubquery(EsqlBaseParser.SubqueryContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#indexPattern}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIndexPattern(EsqlBaseParser.IndexPatternContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#clusterString}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitClusterString(EsqlBaseParser.ClusterStringContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#selectorString}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSelectorString(EsqlBaseParser.SelectorStringContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#unquotedIndexString}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUnquotedIndexString(EsqlBaseParser.UnquotedIndexStringContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#indexString}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIndexString(EsqlBaseParser.IndexStringContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#metadata}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMetadata(EsqlBaseParser.MetadataContext ctx);
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
   * Visit a parse tree produced by {@link EsqlBaseParser#aggFields}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAggFields(EsqlBaseParser.AggFieldsContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#aggField}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAggField(EsqlBaseParser.AggFieldContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#qualifiedName}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQualifiedName(EsqlBaseParser.QualifiedNameContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#fieldName}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFieldName(EsqlBaseParser.FieldNameContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#qualifiedNamePattern}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQualifiedNamePattern(EsqlBaseParser.QualifiedNamePatternContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#fieldNamePattern}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFieldNamePattern(EsqlBaseParser.FieldNamePatternContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#qualifiedNamePatterns}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQualifiedNamePatterns(EsqlBaseParser.QualifiedNamePatternsContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#identifier}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIdentifier(EsqlBaseParser.IdentifierContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#identifierPattern}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIdentifierPattern(EsqlBaseParser.IdentifierPatternContext ctx);
  /**
   * Visit a parse tree produced by the {@code inputParam}
   * labeled alternative in {@link EsqlBaseParser#parameter}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInputParam(EsqlBaseParser.InputParamContext ctx);
  /**
   * Visit a parse tree produced by the {@code inputNamedOrPositionalParam}
   * labeled alternative in {@link EsqlBaseParser#parameter}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInputNamedOrPositionalParam(EsqlBaseParser.InputNamedOrPositionalParamContext ctx);
  /**
   * Visit a parse tree produced by the {@code inputDoubleParams}
   * labeled alternative in {@link EsqlBaseParser#doubleParameter}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInputDoubleParams(EsqlBaseParser.InputDoubleParamsContext ctx);
  /**
   * Visit a parse tree produced by the {@code inputNamedOrPositionalDoubleParams}
   * labeled alternative in {@link EsqlBaseParser#doubleParameter}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInputNamedOrPositionalDoubleParams(EsqlBaseParser.InputNamedOrPositionalDoubleParamsContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#identifierOrParameter}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIdentifierOrParameter(EsqlBaseParser.IdentifierOrParameterContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#stringOrParameter}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStringOrParameter(EsqlBaseParser.StringOrParameterContext ctx);
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
   * Visit a parse tree produced by {@link EsqlBaseParser#keepCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitKeepCommand(EsqlBaseParser.KeepCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#dropCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDropCommand(EsqlBaseParser.DropCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#renameCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRenameCommand(EsqlBaseParser.RenameCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#renameClause}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRenameClause(EsqlBaseParser.RenameClauseContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#dissectCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDissectCommand(EsqlBaseParser.DissectCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#dissectCommandOptions}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDissectCommandOptions(EsqlBaseParser.DissectCommandOptionsContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#dissectCommandOption}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDissectCommandOption(EsqlBaseParser.DissectCommandOptionContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#commandNamedParameters}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCommandNamedParameters(EsqlBaseParser.CommandNamedParametersContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#grokCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitGrokCommand(EsqlBaseParser.GrokCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#mvExpandCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMvExpandCommand(EsqlBaseParser.MvExpandCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#explainCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExplainCommand(EsqlBaseParser.ExplainCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#subqueryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSubqueryExpression(EsqlBaseParser.SubqueryExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code showInfo}
   * labeled alternative in {@link EsqlBaseParser#showCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShowInfo(EsqlBaseParser.ShowInfoContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#enrichCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitEnrichCommand(EsqlBaseParser.EnrichCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#enrichPolicyName}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitEnrichPolicyName(EsqlBaseParser.EnrichPolicyNameContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#enrichWithClause}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitEnrichWithClause(EsqlBaseParser.EnrichWithClauseContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#sampleCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSampleCommand(EsqlBaseParser.SampleCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#changePointCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitChangePointCommand(EsqlBaseParser.ChangePointCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#forkCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitForkCommand(EsqlBaseParser.ForkCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#forkSubQueries}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitForkSubQueries(EsqlBaseParser.ForkSubQueriesContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#forkSubQuery}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitForkSubQuery(EsqlBaseParser.ForkSubQueryContext ctx);
  /**
   * Visit a parse tree produced by the {@code singleForkSubQueryCommand}
   * labeled alternative in {@link EsqlBaseParser#forkSubQueryCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingleForkSubQueryCommand(EsqlBaseParser.SingleForkSubQueryCommandContext ctx);
  /**
   * Visit a parse tree produced by the {@code compositeForkSubQuery}
   * labeled alternative in {@link EsqlBaseParser#forkSubQueryCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCompositeForkSubQuery(EsqlBaseParser.CompositeForkSubQueryContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#forkSubQueryProcessingCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitForkSubQueryProcessingCommand(EsqlBaseParser.ForkSubQueryProcessingCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#rerankCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRerankCommand(EsqlBaseParser.RerankCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#completionCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCompletionCommand(EsqlBaseParser.CompletionCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#inlineStatsCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInlineStatsCommand(EsqlBaseParser.InlineStatsCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#fuseCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFuseCommand(EsqlBaseParser.FuseCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#fuseConfiguration}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFuseConfiguration(EsqlBaseParser.FuseConfigurationContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#fuseKeyByFields}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFuseKeyByFields(EsqlBaseParser.FuseKeyByFieldsContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#lookupCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLookupCommand(EsqlBaseParser.LookupCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#insistCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInsistCommand(EsqlBaseParser.InsistCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#uriPartsCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUriPartsCommand(EsqlBaseParser.UriPartsCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#setCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSetCommand(EsqlBaseParser.SetCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#setField}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSetField(EsqlBaseParser.SetFieldContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#mmrCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMmrCommand(EsqlBaseParser.MmrCommandContext ctx);
  /**
   * Visit a parse tree produced by the {@code mmrQueryVectorParameter}
   * labeled alternative in {@link EsqlBaseParser#mmrQueryVectorParams}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMmrQueryVectorParameter(EsqlBaseParser.MmrQueryVectorParameterContext ctx);
  /**
   * Visit a parse tree produced by the {@code mmrQueryVectorExpression}
   * labeled alternative in {@link EsqlBaseParser#mmrQueryVectorParams}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMmrQueryVectorExpression(EsqlBaseParser.MmrQueryVectorExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code matchExpression}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMatchExpression(EsqlBaseParser.MatchExpressionContext ctx);
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
   * Visit a parse tree produced by the {@code isNull}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIsNull(EsqlBaseParser.IsNullContext ctx);
  /**
   * Visit a parse tree produced by the {@code regexExpression}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRegexExpression(EsqlBaseParser.RegexExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code logicalIn}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLogicalIn(EsqlBaseParser.LogicalInContext ctx);
  /**
   * Visit a parse tree produced by the {@code logicalBinary}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLogicalBinary(EsqlBaseParser.LogicalBinaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code likeExpression}
   * labeled alternative in {@link EsqlBaseParser#regexBooleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLikeExpression(EsqlBaseParser.LikeExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code rlikeExpression}
   * labeled alternative in {@link EsqlBaseParser#regexBooleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRlikeExpression(EsqlBaseParser.RlikeExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code likeListExpression}
   * labeled alternative in {@link EsqlBaseParser#regexBooleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLikeListExpression(EsqlBaseParser.LikeListExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code rlikeListExpression}
   * labeled alternative in {@link EsqlBaseParser#regexBooleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRlikeListExpression(EsqlBaseParser.RlikeListExpressionContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#matchBooleanExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMatchBooleanExpression(EsqlBaseParser.MatchBooleanExpressionContext ctx);
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
   * Visit a parse tree produced by the {@code dereference}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDereference(EsqlBaseParser.DereferenceContext ctx);
  /**
   * Visit a parse tree produced by the {@code inlineCast}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInlineCast(EsqlBaseParser.InlineCastContext ctx);
  /**
   * Visit a parse tree produced by the {@code constantDefault}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitConstantDefault(EsqlBaseParser.ConstantDefaultContext ctx);
  /**
   * Visit a parse tree produced by the {@code parenthesizedExpression}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitParenthesizedExpression(EsqlBaseParser.ParenthesizedExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code function}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunction(EsqlBaseParser.FunctionContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#functionExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunctionExpression(EsqlBaseParser.FunctionExpressionContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#functionName}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunctionName(EsqlBaseParser.FunctionNameContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#mapExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMapExpression(EsqlBaseParser.MapExpressionContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#entryExpression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitEntryExpression(EsqlBaseParser.EntryExpressionContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#mapValue}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMapValue(EsqlBaseParser.MapValueContext ctx);
  /**
   * Visit a parse tree produced by the {@code nullLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNullLiteral(EsqlBaseParser.NullLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code qualifiedIntegerLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQualifiedIntegerLiteral(EsqlBaseParser.QualifiedIntegerLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code decimalLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDecimalLiteral(EsqlBaseParser.DecimalLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code integerLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIntegerLiteral(EsqlBaseParser.IntegerLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code booleanLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBooleanLiteral(EsqlBaseParser.BooleanLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code inputParameter}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInputParameter(EsqlBaseParser.InputParameterContext ctx);
  /**
   * Visit a parse tree produced by the {@code stringLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStringLiteral(EsqlBaseParser.StringLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code numericArrayLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNumericArrayLiteral(EsqlBaseParser.NumericArrayLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code booleanArrayLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBooleanArrayLiteral(EsqlBaseParser.BooleanArrayLiteralContext ctx);
  /**
   * Visit a parse tree produced by the {@code stringArrayLiteral}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStringArrayLiteral(EsqlBaseParser.StringArrayLiteralContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#booleanValue}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBooleanValue(EsqlBaseParser.BooleanValueContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#numericValue}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNumericValue(EsqlBaseParser.NumericValueContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#decimalValue}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDecimalValue(EsqlBaseParser.DecimalValueContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#integerValue}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIntegerValue(EsqlBaseParser.IntegerValueContext ctx);
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
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#joinCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitJoinCommand(EsqlBaseParser.JoinCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#joinTarget}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitJoinTarget(EsqlBaseParser.JoinTargetContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#joinCondition}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitJoinCondition(EsqlBaseParser.JoinConditionContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#promqlCommand}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPromqlCommand(EsqlBaseParser.PromqlCommandContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#valueName}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitValueName(EsqlBaseParser.ValueNameContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#promqlParam}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPromqlParam(EsqlBaseParser.PromqlParamContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#promqlParamName}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPromqlParamName(EsqlBaseParser.PromqlParamNameContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#promqlParamValue}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPromqlParamValue(EsqlBaseParser.PromqlParamValueContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#promqlQueryContent}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPromqlQueryContent(EsqlBaseParser.PromqlQueryContentContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#promqlQueryPart}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPromqlQueryPart(EsqlBaseParser.PromqlQueryPartContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#promqlIndexPattern}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPromqlIndexPattern(EsqlBaseParser.PromqlIndexPatternContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#promqlClusterString}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPromqlClusterString(EsqlBaseParser.PromqlClusterStringContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#promqlSelectorString}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPromqlSelectorString(EsqlBaseParser.PromqlSelectorStringContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#promqlUnquotedIndexString}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPromqlUnquotedIndexString(EsqlBaseParser.PromqlUnquotedIndexStringContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#promqlIndexString}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPromqlIndexString(EsqlBaseParser.PromqlIndexStringContext ctx);
}
