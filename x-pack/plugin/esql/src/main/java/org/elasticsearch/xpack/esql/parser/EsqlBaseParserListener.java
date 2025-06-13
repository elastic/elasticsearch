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
   * Enter a parse tree produced by the {@code toDataType}
   * labeled alternative in {@link EsqlBaseParser#dataType}.
   * @param ctx the parse tree
   */
  void enterToDataType(EsqlBaseParser.ToDataTypeContext ctx);
  /**
   * Exit a parse tree produced by the {@code toDataType}
   * labeled alternative in {@link EsqlBaseParser#dataType}.
   * @param ctx the parse tree
   */
  void exitToDataType(EsqlBaseParser.ToDataTypeContext ctx);
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
   * Enter a parse tree produced by {@link EsqlBaseParser#rerankFields}.
   * @param ctx the parse tree
   */
  void enterRerankFields(EsqlBaseParser.RerankFieldsContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#rerankFields}.
   * @param ctx the parse tree
   */
  void exitRerankFields(EsqlBaseParser.RerankFieldsContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#rerankField}.
   * @param ctx the parse tree
   */
  void enterRerankField(EsqlBaseParser.RerankFieldContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#rerankField}.
   * @param ctx the parse tree
   */
  void exitRerankField(EsqlBaseParser.RerankFieldContext ctx);
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
   * Enter a parse tree produced by {@link EsqlBaseParser#timeSeriesCommand}.
   * @param ctx the parse tree
   */
  void enterTimeSeriesCommand(EsqlBaseParser.TimeSeriesCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#timeSeriesCommand}.
   * @param ctx the parse tree
   */
  void exitTimeSeriesCommand(EsqlBaseParser.TimeSeriesCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#indexPatternAndMetadataFields}.
   * @param ctx the parse tree
   */
  void enterIndexPatternAndMetadataFields(EsqlBaseParser.IndexPatternAndMetadataFieldsContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#indexPatternAndMetadataFields}.
   * @param ctx the parse tree
   */
  void exitIndexPatternAndMetadataFields(EsqlBaseParser.IndexPatternAndMetadataFieldsContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#indexPattern}.
   * @param ctx the parse tree
   */
  void enterIndexPattern(EsqlBaseParser.IndexPatternContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#indexPattern}.
   * @param ctx the parse tree
   */
  void exitIndexPattern(EsqlBaseParser.IndexPatternContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#clusterString}.
   * @param ctx the parse tree
   */
  void enterClusterString(EsqlBaseParser.ClusterStringContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#clusterString}.
   * @param ctx the parse tree
   */
  void exitClusterString(EsqlBaseParser.ClusterStringContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#selectorString}.
   * @param ctx the parse tree
   */
  void enterSelectorString(EsqlBaseParser.SelectorStringContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#selectorString}.
   * @param ctx the parse tree
   */
  void exitSelectorString(EsqlBaseParser.SelectorStringContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#indexString}.
   * @param ctx the parse tree
   */
  void enterIndexString(EsqlBaseParser.IndexStringContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#indexString}.
   * @param ctx the parse tree
   */
  void exitIndexString(EsqlBaseParser.IndexStringContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#metadata}.
   * @param ctx the parse tree
   */
  void enterMetadata(EsqlBaseParser.MetadataContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#metadata}.
   * @param ctx the parse tree
   */
  void exitMetadata(EsqlBaseParser.MetadataContext ctx);
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
   * Enter a parse tree produced by {@link EsqlBaseParser#aggFields}.
   * @param ctx the parse tree
   */
  void enterAggFields(EsqlBaseParser.AggFieldsContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#aggFields}.
   * @param ctx the parse tree
   */
  void exitAggFields(EsqlBaseParser.AggFieldsContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#aggField}.
   * @param ctx the parse tree
   */
  void enterAggField(EsqlBaseParser.AggFieldContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#aggField}.
   * @param ctx the parse tree
   */
  void exitAggField(EsqlBaseParser.AggFieldContext ctx);
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
   * Enter a parse tree produced by {@link EsqlBaseParser#qualifiedNamePattern}.
   * @param ctx the parse tree
   */
  void enterQualifiedNamePattern(EsqlBaseParser.QualifiedNamePatternContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#qualifiedNamePattern}.
   * @param ctx the parse tree
   */
  void exitQualifiedNamePattern(EsqlBaseParser.QualifiedNamePatternContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#qualifiedNamePatterns}.
   * @param ctx the parse tree
   */
  void enterQualifiedNamePatterns(EsqlBaseParser.QualifiedNamePatternsContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#qualifiedNamePatterns}.
   * @param ctx the parse tree
   */
  void exitQualifiedNamePatterns(EsqlBaseParser.QualifiedNamePatternsContext ctx);
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
   * Enter a parse tree produced by {@link EsqlBaseParser#identifierPattern}.
   * @param ctx the parse tree
   */
  void enterIdentifierPattern(EsqlBaseParser.IdentifierPatternContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#identifierPattern}.
   * @param ctx the parse tree
   */
  void exitIdentifierPattern(EsqlBaseParser.IdentifierPatternContext ctx);
  /**
   * Enter a parse tree produced by the {@code inputParam}
   * labeled alternative in {@link EsqlBaseParser#parameter}.
   * @param ctx the parse tree
   */
  void enterInputParam(EsqlBaseParser.InputParamContext ctx);
  /**
   * Exit a parse tree produced by the {@code inputParam}
   * labeled alternative in {@link EsqlBaseParser#parameter}.
   * @param ctx the parse tree
   */
  void exitInputParam(EsqlBaseParser.InputParamContext ctx);
  /**
   * Enter a parse tree produced by the {@code inputNamedOrPositionalParam}
   * labeled alternative in {@link EsqlBaseParser#parameter}.
   * @param ctx the parse tree
   */
  void enterInputNamedOrPositionalParam(EsqlBaseParser.InputNamedOrPositionalParamContext ctx);
  /**
   * Exit a parse tree produced by the {@code inputNamedOrPositionalParam}
   * labeled alternative in {@link EsqlBaseParser#parameter}.
   * @param ctx the parse tree
   */
  void exitInputNamedOrPositionalParam(EsqlBaseParser.InputNamedOrPositionalParamContext ctx);
  /**
   * Enter a parse tree produced by the {@code inputDoubleParams}
   * labeled alternative in {@link EsqlBaseParser#doubleParameter}.
   * @param ctx the parse tree
   */
  void enterInputDoubleParams(EsqlBaseParser.InputDoubleParamsContext ctx);
  /**
   * Exit a parse tree produced by the {@code inputDoubleParams}
   * labeled alternative in {@link EsqlBaseParser#doubleParameter}.
   * @param ctx the parse tree
   */
  void exitInputDoubleParams(EsqlBaseParser.InputDoubleParamsContext ctx);
  /**
   * Enter a parse tree produced by the {@code inputNamedOrPositionalDoubleParams}
   * labeled alternative in {@link EsqlBaseParser#doubleParameter}.
   * @param ctx the parse tree
   */
  void enterInputNamedOrPositionalDoubleParams(EsqlBaseParser.InputNamedOrPositionalDoubleParamsContext ctx);
  /**
   * Exit a parse tree produced by the {@code inputNamedOrPositionalDoubleParams}
   * labeled alternative in {@link EsqlBaseParser#doubleParameter}.
   * @param ctx the parse tree
   */
  void exitInputNamedOrPositionalDoubleParams(EsqlBaseParser.InputNamedOrPositionalDoubleParamsContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#identifierOrParameter}.
   * @param ctx the parse tree
   */
  void enterIdentifierOrParameter(EsqlBaseParser.IdentifierOrParameterContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#identifierOrParameter}.
   * @param ctx the parse tree
   */
  void exitIdentifierOrParameter(EsqlBaseParser.IdentifierOrParameterContext ctx);
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
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#lookupCommand}.
   * @param ctx the parse tree
   */
  void enterLookupCommand(EsqlBaseParser.LookupCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#lookupCommand}.
   * @param ctx the parse tree
   */
  void exitLookupCommand(EsqlBaseParser.LookupCommandContext ctx);
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
   * Enter a parse tree produced by {@link EsqlBaseParser#changePointCommand}.
   * @param ctx the parse tree
   */
  void enterChangePointCommand(EsqlBaseParser.ChangePointCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#changePointCommand}.
   * @param ctx the parse tree
   */
  void exitChangePointCommand(EsqlBaseParser.ChangePointCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#insistCommand}.
   * @param ctx the parse tree
   */
  void enterInsistCommand(EsqlBaseParser.InsistCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#insistCommand}.
   * @param ctx the parse tree
   */
  void exitInsistCommand(EsqlBaseParser.InsistCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#forkCommand}.
   * @param ctx the parse tree
   */
  void enterForkCommand(EsqlBaseParser.ForkCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#forkCommand}.
   * @param ctx the parse tree
   */
  void exitForkCommand(EsqlBaseParser.ForkCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#forkSubQueries}.
   * @param ctx the parse tree
   */
  void enterForkSubQueries(EsqlBaseParser.ForkSubQueriesContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#forkSubQueries}.
   * @param ctx the parse tree
   */
  void exitForkSubQueries(EsqlBaseParser.ForkSubQueriesContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#forkSubQuery}.
   * @param ctx the parse tree
   */
  void enterForkSubQuery(EsqlBaseParser.ForkSubQueryContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#forkSubQuery}.
   * @param ctx the parse tree
   */
  void exitForkSubQuery(EsqlBaseParser.ForkSubQueryContext ctx);
  /**
   * Enter a parse tree produced by the {@code singleForkSubQueryCommand}
   * labeled alternative in {@link EsqlBaseParser#forkSubQueryCommand}.
   * @param ctx the parse tree
   */
  void enterSingleForkSubQueryCommand(EsqlBaseParser.SingleForkSubQueryCommandContext ctx);
  /**
   * Exit a parse tree produced by the {@code singleForkSubQueryCommand}
   * labeled alternative in {@link EsqlBaseParser#forkSubQueryCommand}.
   * @param ctx the parse tree
   */
  void exitSingleForkSubQueryCommand(EsqlBaseParser.SingleForkSubQueryCommandContext ctx);
  /**
   * Enter a parse tree produced by the {@code compositeForkSubQuery}
   * labeled alternative in {@link EsqlBaseParser#forkSubQueryCommand}.
   * @param ctx the parse tree
   */
  void enterCompositeForkSubQuery(EsqlBaseParser.CompositeForkSubQueryContext ctx);
  /**
   * Exit a parse tree produced by the {@code compositeForkSubQuery}
   * labeled alternative in {@link EsqlBaseParser#forkSubQueryCommand}.
   * @param ctx the parse tree
   */
  void exitCompositeForkSubQuery(EsqlBaseParser.CompositeForkSubQueryContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#forkSubQueryProcessingCommand}.
   * @param ctx the parse tree
   */
  void enterForkSubQueryProcessingCommand(EsqlBaseParser.ForkSubQueryProcessingCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#forkSubQueryProcessingCommand}.
   * @param ctx the parse tree
   */
  void exitForkSubQueryProcessingCommand(EsqlBaseParser.ForkSubQueryProcessingCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#rrfCommand}.
   * @param ctx the parse tree
   */
  void enterRrfCommand(EsqlBaseParser.RrfCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#rrfCommand}.
   * @param ctx the parse tree
   */
  void exitRrfCommand(EsqlBaseParser.RrfCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#rerankCommand}.
   * @param ctx the parse tree
   */
  void enterRerankCommand(EsqlBaseParser.RerankCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#rerankCommand}.
   * @param ctx the parse tree
   */
  void exitRerankCommand(EsqlBaseParser.RerankCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#completionCommand}.
   * @param ctx the parse tree
   */
  void enterCompletionCommand(EsqlBaseParser.CompletionCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#completionCommand}.
   * @param ctx the parse tree
   */
  void exitCompletionCommand(EsqlBaseParser.CompletionCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#sampleCommand}.
   * @param ctx the parse tree
   */
  void enterSampleCommand(EsqlBaseParser.SampleCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#sampleCommand}.
   * @param ctx the parse tree
   */
  void exitSampleCommand(EsqlBaseParser.SampleCommandContext ctx);
  /**
   * Enter a parse tree produced by the {@code matchExpression}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   */
  void enterMatchExpression(EsqlBaseParser.MatchExpressionContext ctx);
  /**
   * Exit a parse tree produced by the {@code matchExpression}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   */
  void exitMatchExpression(EsqlBaseParser.MatchExpressionContext ctx);
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
   * Enter a parse tree produced by the {@code isNull}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   */
  void enterIsNull(EsqlBaseParser.IsNullContext ctx);
  /**
   * Exit a parse tree produced by the {@code isNull}
   * labeled alternative in {@link EsqlBaseParser#booleanExpression}.
   * @param ctx the parse tree
   */
  void exitIsNull(EsqlBaseParser.IsNullContext ctx);
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
   * Enter a parse tree produced by the {@code likeExpression}
   * labeled alternative in {@link EsqlBaseParser#regexBooleanExpression}.
   * @param ctx the parse tree
   */
  void enterLikeExpression(EsqlBaseParser.LikeExpressionContext ctx);
  /**
   * Exit a parse tree produced by the {@code likeExpression}
   * labeled alternative in {@link EsqlBaseParser#regexBooleanExpression}.
   * @param ctx the parse tree
   */
  void exitLikeExpression(EsqlBaseParser.LikeExpressionContext ctx);
  /**
   * Enter a parse tree produced by the {@code rlikeExpression}
   * labeled alternative in {@link EsqlBaseParser#regexBooleanExpression}.
   * @param ctx the parse tree
   */
  void enterRlikeExpression(EsqlBaseParser.RlikeExpressionContext ctx);
  /**
   * Exit a parse tree produced by the {@code rlikeExpression}
   * labeled alternative in {@link EsqlBaseParser#regexBooleanExpression}.
   * @param ctx the parse tree
   */
  void exitRlikeExpression(EsqlBaseParser.RlikeExpressionContext ctx);
  /**
   * Enter a parse tree produced by the {@code logicalLikeList}
   * labeled alternative in {@link EsqlBaseParser#regexBooleanExpression}.
   * @param ctx the parse tree
   */
  void enterLogicalLikeList(EsqlBaseParser.LogicalLikeListContext ctx);
  /**
   * Exit a parse tree produced by the {@code logicalLikeList}
   * labeled alternative in {@link EsqlBaseParser#regexBooleanExpression}.
   * @param ctx the parse tree
   */
  void exitLogicalLikeList(EsqlBaseParser.LogicalLikeListContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#matchBooleanExpression}.
   * @param ctx the parse tree
   */
  void enterMatchBooleanExpression(EsqlBaseParser.MatchBooleanExpressionContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#matchBooleanExpression}.
   * @param ctx the parse tree
   */
  void exitMatchBooleanExpression(EsqlBaseParser.MatchBooleanExpressionContext ctx);
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
   * Enter a parse tree produced by the {@code inlineCast}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   */
  void enterInlineCast(EsqlBaseParser.InlineCastContext ctx);
  /**
   * Exit a parse tree produced by the {@code inlineCast}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   */
  void exitInlineCast(EsqlBaseParser.InlineCastContext ctx);
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
   * Enter a parse tree produced by the {@code function}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   */
  void enterFunction(EsqlBaseParser.FunctionContext ctx);
  /**
   * Exit a parse tree produced by the {@code function}
   * labeled alternative in {@link EsqlBaseParser#primaryExpression}.
   * @param ctx the parse tree
   */
  void exitFunction(EsqlBaseParser.FunctionContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#functionExpression}.
   * @param ctx the parse tree
   */
  void enterFunctionExpression(EsqlBaseParser.FunctionExpressionContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#functionExpression}.
   * @param ctx the parse tree
   */
  void exitFunctionExpression(EsqlBaseParser.FunctionExpressionContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#functionName}.
   * @param ctx the parse tree
   */
  void enterFunctionName(EsqlBaseParser.FunctionNameContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#functionName}.
   * @param ctx the parse tree
   */
  void exitFunctionName(EsqlBaseParser.FunctionNameContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#mapExpression}.
   * @param ctx the parse tree
   */
  void enterMapExpression(EsqlBaseParser.MapExpressionContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#mapExpression}.
   * @param ctx the parse tree
   */
  void exitMapExpression(EsqlBaseParser.MapExpressionContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#entryExpression}.
   * @param ctx the parse tree
   */
  void enterEntryExpression(EsqlBaseParser.EntryExpressionContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#entryExpression}.
   * @param ctx the parse tree
   */
  void exitEntryExpression(EsqlBaseParser.EntryExpressionContext ctx);
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
   * Enter a parse tree produced by the {@code inputParameter}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void enterInputParameter(EsqlBaseParser.InputParameterContext ctx);
  /**
   * Exit a parse tree produced by the {@code inputParameter}
   * labeled alternative in {@link EsqlBaseParser#constant}.
   * @param ctx the parse tree
   */
  void exitInputParameter(EsqlBaseParser.InputParameterContext ctx);
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
   * Enter a parse tree produced by {@link EsqlBaseParser#joinCommand}.
   * @param ctx the parse tree
   */
  void enterJoinCommand(EsqlBaseParser.JoinCommandContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#joinCommand}.
   * @param ctx the parse tree
   */
  void exitJoinCommand(EsqlBaseParser.JoinCommandContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#joinTarget}.
   * @param ctx the parse tree
   */
  void enterJoinTarget(EsqlBaseParser.JoinTargetContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#joinTarget}.
   * @param ctx the parse tree
   */
  void exitJoinTarget(EsqlBaseParser.JoinTargetContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#joinCondition}.
   * @param ctx the parse tree
   */
  void enterJoinCondition(EsqlBaseParser.JoinConditionContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#joinCondition}.
   * @param ctx the parse tree
   */
  void exitJoinCondition(EsqlBaseParser.JoinConditionContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#joinPredicate}.
   * @param ctx the parse tree
   */
  void enterJoinPredicate(EsqlBaseParser.JoinPredicateContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#joinPredicate}.
   * @param ctx the parse tree
   */
  void exitJoinPredicate(EsqlBaseParser.JoinPredicateContext ctx);
}
