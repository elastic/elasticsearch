// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.sql.parser;

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link SqlBaseParser}.
 */
interface SqlBaseListener extends ParseTreeListener {
    /**
     * Enter a parse tree produced by {@link SqlBaseParser#singleStatement}.
     * @param ctx the parse tree
     */
    void enterSingleStatement(SqlBaseParser.SingleStatementContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#singleStatement}.
     * @param ctx the parse tree
     */
    void exitSingleStatement(SqlBaseParser.SingleStatementContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#singleExpression}.
     * @param ctx the parse tree
     */
    void enterSingleExpression(SqlBaseParser.SingleExpressionContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#singleExpression}.
     * @param ctx the parse tree
     */
    void exitSingleExpression(SqlBaseParser.SingleExpressionContext ctx);

    /**
     * Enter a parse tree produced by the {@code statementDefault}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void enterStatementDefault(SqlBaseParser.StatementDefaultContext ctx);

    /**
     * Exit a parse tree produced by the {@code statementDefault}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void exitStatementDefault(SqlBaseParser.StatementDefaultContext ctx);

    /**
     * Enter a parse tree produced by the {@code explain}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void enterExplain(SqlBaseParser.ExplainContext ctx);

    /**
     * Exit a parse tree produced by the {@code explain}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void exitExplain(SqlBaseParser.ExplainContext ctx);

    /**
     * Enter a parse tree produced by the {@code debug}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void enterDebug(SqlBaseParser.DebugContext ctx);

    /**
     * Exit a parse tree produced by the {@code debug}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void exitDebug(SqlBaseParser.DebugContext ctx);

    /**
     * Enter a parse tree produced by the {@code showTables}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void enterShowTables(SqlBaseParser.ShowTablesContext ctx);

    /**
     * Exit a parse tree produced by the {@code showTables}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void exitShowTables(SqlBaseParser.ShowTablesContext ctx);

    /**
     * Enter a parse tree produced by the {@code showColumns}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void enterShowColumns(SqlBaseParser.ShowColumnsContext ctx);

    /**
     * Exit a parse tree produced by the {@code showColumns}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void exitShowColumns(SqlBaseParser.ShowColumnsContext ctx);

    /**
     * Enter a parse tree produced by the {@code showFunctions}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void enterShowFunctions(SqlBaseParser.ShowFunctionsContext ctx);

    /**
     * Exit a parse tree produced by the {@code showFunctions}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void exitShowFunctions(SqlBaseParser.ShowFunctionsContext ctx);

    /**
     * Enter a parse tree produced by the {@code showSchemas}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void enterShowSchemas(SqlBaseParser.ShowSchemasContext ctx);

    /**
     * Exit a parse tree produced by the {@code showSchemas}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void exitShowSchemas(SqlBaseParser.ShowSchemasContext ctx);

    /**
     * Enter a parse tree produced by the {@code showCatalogs}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void enterShowCatalogs(SqlBaseParser.ShowCatalogsContext ctx);

    /**
     * Exit a parse tree produced by the {@code showCatalogs}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void exitShowCatalogs(SqlBaseParser.ShowCatalogsContext ctx);

    /**
     * Enter a parse tree produced by the {@code sysTables}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void enterSysTables(SqlBaseParser.SysTablesContext ctx);

    /**
     * Exit a parse tree produced by the {@code sysTables}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void exitSysTables(SqlBaseParser.SysTablesContext ctx);

    /**
     * Enter a parse tree produced by the {@code sysColumns}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void enterSysColumns(SqlBaseParser.SysColumnsContext ctx);

    /**
     * Exit a parse tree produced by the {@code sysColumns}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void exitSysColumns(SqlBaseParser.SysColumnsContext ctx);

    /**
     * Enter a parse tree produced by the {@code sysTypes}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void enterSysTypes(SqlBaseParser.SysTypesContext ctx);

    /**
     * Exit a parse tree produced by the {@code sysTypes}
     * labeled alternative in {@link SqlBaseParser#statement}.
     * @param ctx the parse tree
     */
    void exitSysTypes(SqlBaseParser.SysTypesContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#query}.
     * @param ctx the parse tree
     */
    void enterQuery(SqlBaseParser.QueryContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#query}.
     * @param ctx the parse tree
     */
    void exitQuery(SqlBaseParser.QueryContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#queryNoWith}.
     * @param ctx the parse tree
     */
    void enterQueryNoWith(SqlBaseParser.QueryNoWithContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#queryNoWith}.
     * @param ctx the parse tree
     */
    void exitQueryNoWith(SqlBaseParser.QueryNoWithContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#limitClause}.
     * @param ctx the parse tree
     */
    void enterLimitClause(SqlBaseParser.LimitClauseContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#limitClause}.
     * @param ctx the parse tree
     */
    void exitLimitClause(SqlBaseParser.LimitClauseContext ctx);

    /**
     * Enter a parse tree produced by the {@code queryPrimaryDefault}
     * labeled alternative in {@link SqlBaseParser#queryTerm}.
     * @param ctx the parse tree
     */
    void enterQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx);

    /**
     * Exit a parse tree produced by the {@code queryPrimaryDefault}
     * labeled alternative in {@link SqlBaseParser#queryTerm}.
     * @param ctx the parse tree
     */
    void exitQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx);

    /**
     * Enter a parse tree produced by the {@code subquery}
     * labeled alternative in {@link SqlBaseParser#queryTerm}.
     * @param ctx the parse tree
     */
    void enterSubquery(SqlBaseParser.SubqueryContext ctx);

    /**
     * Exit a parse tree produced by the {@code subquery}
     * labeled alternative in {@link SqlBaseParser#queryTerm}.
     * @param ctx the parse tree
     */
    void exitSubquery(SqlBaseParser.SubqueryContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#orderBy}.
     * @param ctx the parse tree
     */
    void enterOrderBy(SqlBaseParser.OrderByContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#orderBy}.
     * @param ctx the parse tree
     */
    void exitOrderBy(SqlBaseParser.OrderByContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#querySpecification}.
     * @param ctx the parse tree
     */
    void enterQuerySpecification(SqlBaseParser.QuerySpecificationContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#querySpecification}.
     * @param ctx the parse tree
     */
    void exitQuerySpecification(SqlBaseParser.QuerySpecificationContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#fromClause}.
     * @param ctx the parse tree
     */
    void enterFromClause(SqlBaseParser.FromClauseContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#fromClause}.
     * @param ctx the parse tree
     */
    void exitFromClause(SqlBaseParser.FromClauseContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#groupBy}.
     * @param ctx the parse tree
     */
    void enterGroupBy(SqlBaseParser.GroupByContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#groupBy}.
     * @param ctx the parse tree
     */
    void exitGroupBy(SqlBaseParser.GroupByContext ctx);

    /**
     * Enter a parse tree produced by the {@code singleGroupingSet}
     * labeled alternative in {@link SqlBaseParser#groupingElement}.
     * @param ctx the parse tree
     */
    void enterSingleGroupingSet(SqlBaseParser.SingleGroupingSetContext ctx);

    /**
     * Exit a parse tree produced by the {@code singleGroupingSet}
     * labeled alternative in {@link SqlBaseParser#groupingElement}.
     * @param ctx the parse tree
     */
    void exitSingleGroupingSet(SqlBaseParser.SingleGroupingSetContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#groupingExpressions}.
     * @param ctx the parse tree
     */
    void enterGroupingExpressions(SqlBaseParser.GroupingExpressionsContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#groupingExpressions}.
     * @param ctx the parse tree
     */
    void exitGroupingExpressions(SqlBaseParser.GroupingExpressionsContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#namedQuery}.
     * @param ctx the parse tree
     */
    void enterNamedQuery(SqlBaseParser.NamedQueryContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#namedQuery}.
     * @param ctx the parse tree
     */
    void exitNamedQuery(SqlBaseParser.NamedQueryContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#topClause}.
     * @param ctx the parse tree
     */
    void enterTopClause(SqlBaseParser.TopClauseContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#topClause}.
     * @param ctx the parse tree
     */
    void exitTopClause(SqlBaseParser.TopClauseContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#setQuantifier}.
     * @param ctx the parse tree
     */
    void enterSetQuantifier(SqlBaseParser.SetQuantifierContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#setQuantifier}.
     * @param ctx the parse tree
     */
    void exitSetQuantifier(SqlBaseParser.SetQuantifierContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#selectItems}.
     * @param ctx the parse tree
     */
    void enterSelectItems(SqlBaseParser.SelectItemsContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#selectItems}.
     * @param ctx the parse tree
     */
    void exitSelectItems(SqlBaseParser.SelectItemsContext ctx);

    /**
     * Enter a parse tree produced by the {@code selectExpression}
     * labeled alternative in {@link SqlBaseParser#selectItem}.
     * @param ctx the parse tree
     */
    void enterSelectExpression(SqlBaseParser.SelectExpressionContext ctx);

    /**
     * Exit a parse tree produced by the {@code selectExpression}
     * labeled alternative in {@link SqlBaseParser#selectItem}.
     * @param ctx the parse tree
     */
    void exitSelectExpression(SqlBaseParser.SelectExpressionContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#relation}.
     * @param ctx the parse tree
     */
    void enterRelation(SqlBaseParser.RelationContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#relation}.
     * @param ctx the parse tree
     */
    void exitRelation(SqlBaseParser.RelationContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#joinRelation}.
     * @param ctx the parse tree
     */
    void enterJoinRelation(SqlBaseParser.JoinRelationContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#joinRelation}.
     * @param ctx the parse tree
     */
    void exitJoinRelation(SqlBaseParser.JoinRelationContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#joinType}.
     * @param ctx the parse tree
     */
    void enterJoinType(SqlBaseParser.JoinTypeContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#joinType}.
     * @param ctx the parse tree
     */
    void exitJoinType(SqlBaseParser.JoinTypeContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#joinCriteria}.
     * @param ctx the parse tree
     */
    void enterJoinCriteria(SqlBaseParser.JoinCriteriaContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#joinCriteria}.
     * @param ctx the parse tree
     */
    void exitJoinCriteria(SqlBaseParser.JoinCriteriaContext ctx);

    /**
     * Enter a parse tree produced by the {@code tableName}
     * labeled alternative in {@link SqlBaseParser#relationPrimary}.
     * @param ctx the parse tree
     */
    void enterTableName(SqlBaseParser.TableNameContext ctx);

    /**
     * Exit a parse tree produced by the {@code tableName}
     * labeled alternative in {@link SqlBaseParser#relationPrimary}.
     * @param ctx the parse tree
     */
    void exitTableName(SqlBaseParser.TableNameContext ctx);

    /**
     * Enter a parse tree produced by the {@code aliasedQuery}
     * labeled alternative in {@link SqlBaseParser#relationPrimary}.
     * @param ctx the parse tree
     */
    void enterAliasedQuery(SqlBaseParser.AliasedQueryContext ctx);

    /**
     * Exit a parse tree produced by the {@code aliasedQuery}
     * labeled alternative in {@link SqlBaseParser#relationPrimary}.
     * @param ctx the parse tree
     */
    void exitAliasedQuery(SqlBaseParser.AliasedQueryContext ctx);

    /**
     * Enter a parse tree produced by the {@code aliasedRelation}
     * labeled alternative in {@link SqlBaseParser#relationPrimary}.
     * @param ctx the parse tree
     */
    void enterAliasedRelation(SqlBaseParser.AliasedRelationContext ctx);

    /**
     * Exit a parse tree produced by the {@code aliasedRelation}
     * labeled alternative in {@link SqlBaseParser#relationPrimary}.
     * @param ctx the parse tree
     */
    void exitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#pivotClause}.
     * @param ctx the parse tree
     */
    void enterPivotClause(SqlBaseParser.PivotClauseContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#pivotClause}.
     * @param ctx the parse tree
     */
    void exitPivotClause(SqlBaseParser.PivotClauseContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#pivotArgs}.
     * @param ctx the parse tree
     */
    void enterPivotArgs(SqlBaseParser.PivotArgsContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#pivotArgs}.
     * @param ctx the parse tree
     */
    void exitPivotArgs(SqlBaseParser.PivotArgsContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#namedValueExpression}.
     * @param ctx the parse tree
     */
    void enterNamedValueExpression(SqlBaseParser.NamedValueExpressionContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#namedValueExpression}.
     * @param ctx the parse tree
     */
    void exitNamedValueExpression(SqlBaseParser.NamedValueExpressionContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#expression}.
     * @param ctx the parse tree
     */
    void enterExpression(SqlBaseParser.ExpressionContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#expression}.
     * @param ctx the parse tree
     */
    void exitExpression(SqlBaseParser.ExpressionContext ctx);

    /**
     * Enter a parse tree produced by the {@code logicalNot}
     * labeled alternative in {@link SqlBaseParser#booleanExpression}.
     * @param ctx the parse tree
     */
    void enterLogicalNot(SqlBaseParser.LogicalNotContext ctx);

    /**
     * Exit a parse tree produced by the {@code logicalNot}
     * labeled alternative in {@link SqlBaseParser#booleanExpression}.
     * @param ctx the parse tree
     */
    void exitLogicalNot(SqlBaseParser.LogicalNotContext ctx);

    /**
     * Enter a parse tree produced by the {@code stringQuery}
     * labeled alternative in {@link SqlBaseParser#booleanExpression}.
     * @param ctx the parse tree
     */
    void enterStringQuery(SqlBaseParser.StringQueryContext ctx);

    /**
     * Exit a parse tree produced by the {@code stringQuery}
     * labeled alternative in {@link SqlBaseParser#booleanExpression}.
     * @param ctx the parse tree
     */
    void exitStringQuery(SqlBaseParser.StringQueryContext ctx);

    /**
     * Enter a parse tree produced by the {@code booleanDefault}
     * labeled alternative in {@link SqlBaseParser#booleanExpression}.
     * @param ctx the parse tree
     */
    void enterBooleanDefault(SqlBaseParser.BooleanDefaultContext ctx);

    /**
     * Exit a parse tree produced by the {@code booleanDefault}
     * labeled alternative in {@link SqlBaseParser#booleanExpression}.
     * @param ctx the parse tree
     */
    void exitBooleanDefault(SqlBaseParser.BooleanDefaultContext ctx);

    /**
     * Enter a parse tree produced by the {@code exists}
     * labeled alternative in {@link SqlBaseParser#booleanExpression}.
     * @param ctx the parse tree
     */
    void enterExists(SqlBaseParser.ExistsContext ctx);

    /**
     * Exit a parse tree produced by the {@code exists}
     * labeled alternative in {@link SqlBaseParser#booleanExpression}.
     * @param ctx the parse tree
     */
    void exitExists(SqlBaseParser.ExistsContext ctx);

    /**
     * Enter a parse tree produced by the {@code multiMatchQuery}
     * labeled alternative in {@link SqlBaseParser#booleanExpression}.
     * @param ctx the parse tree
     */
    void enterMultiMatchQuery(SqlBaseParser.MultiMatchQueryContext ctx);

    /**
     * Exit a parse tree produced by the {@code multiMatchQuery}
     * labeled alternative in {@link SqlBaseParser#booleanExpression}.
     * @param ctx the parse tree
     */
    void exitMultiMatchQuery(SqlBaseParser.MultiMatchQueryContext ctx);

    /**
     * Enter a parse tree produced by the {@code matchQuery}
     * labeled alternative in {@link SqlBaseParser#booleanExpression}.
     * @param ctx the parse tree
     */
    void enterMatchQuery(SqlBaseParser.MatchQueryContext ctx);

    /**
     * Exit a parse tree produced by the {@code matchQuery}
     * labeled alternative in {@link SqlBaseParser#booleanExpression}.
     * @param ctx the parse tree
     */
    void exitMatchQuery(SqlBaseParser.MatchQueryContext ctx);

    /**
     * Enter a parse tree produced by the {@code logicalBinary}
     * labeled alternative in {@link SqlBaseParser#booleanExpression}.
     * @param ctx the parse tree
     */
    void enterLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx);

    /**
     * Exit a parse tree produced by the {@code logicalBinary}
     * labeled alternative in {@link SqlBaseParser#booleanExpression}.
     * @param ctx the parse tree
     */
    void exitLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#matchQueryOptions}.
     * @param ctx the parse tree
     */
    void enterMatchQueryOptions(SqlBaseParser.MatchQueryOptionsContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#matchQueryOptions}.
     * @param ctx the parse tree
     */
    void exitMatchQueryOptions(SqlBaseParser.MatchQueryOptionsContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#predicated}.
     * @param ctx the parse tree
     */
    void enterPredicated(SqlBaseParser.PredicatedContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#predicated}.
     * @param ctx the parse tree
     */
    void exitPredicated(SqlBaseParser.PredicatedContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#predicate}.
     * @param ctx the parse tree
     */
    void enterPredicate(SqlBaseParser.PredicateContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#predicate}.
     * @param ctx the parse tree
     */
    void exitPredicate(SqlBaseParser.PredicateContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#likePattern}.
     * @param ctx the parse tree
     */
    void enterLikePattern(SqlBaseParser.LikePatternContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#likePattern}.
     * @param ctx the parse tree
     */
    void exitLikePattern(SqlBaseParser.LikePatternContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#pattern}.
     * @param ctx the parse tree
     */
    void enterPattern(SqlBaseParser.PatternContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#pattern}.
     * @param ctx the parse tree
     */
    void exitPattern(SqlBaseParser.PatternContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#patternEscape}.
     * @param ctx the parse tree
     */
    void enterPatternEscape(SqlBaseParser.PatternEscapeContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#patternEscape}.
     * @param ctx the parse tree
     */
    void exitPatternEscape(SqlBaseParser.PatternEscapeContext ctx);

    /**
     * Enter a parse tree produced by the {@code valueExpressionDefault}
     * labeled alternative in {@link SqlBaseParser#valueExpression}.
     * @param ctx the parse tree
     */
    void enterValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx);

    /**
     * Exit a parse tree produced by the {@code valueExpressionDefault}
     * labeled alternative in {@link SqlBaseParser#valueExpression}.
     * @param ctx the parse tree
     */
    void exitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx);

    /**
     * Enter a parse tree produced by the {@code comparison}
     * labeled alternative in {@link SqlBaseParser#valueExpression}.
     * @param ctx the parse tree
     */
    void enterComparison(SqlBaseParser.ComparisonContext ctx);

    /**
     * Exit a parse tree produced by the {@code comparison}
     * labeled alternative in {@link SqlBaseParser#valueExpression}.
     * @param ctx the parse tree
     */
    void exitComparison(SqlBaseParser.ComparisonContext ctx);

    /**
     * Enter a parse tree produced by the {@code arithmeticBinary}
     * labeled alternative in {@link SqlBaseParser#valueExpression}.
     * @param ctx the parse tree
     */
    void enterArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx);

    /**
     * Exit a parse tree produced by the {@code arithmeticBinary}
     * labeled alternative in {@link SqlBaseParser#valueExpression}.
     * @param ctx the parse tree
     */
    void exitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx);

    /**
     * Enter a parse tree produced by the {@code arithmeticUnary}
     * labeled alternative in {@link SqlBaseParser#valueExpression}.
     * @param ctx the parse tree
     */
    void enterArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx);

    /**
     * Exit a parse tree produced by the {@code arithmeticUnary}
     * labeled alternative in {@link SqlBaseParser#valueExpression}.
     * @param ctx the parse tree
     */
    void exitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx);

    /**
     * Enter a parse tree produced by the {@code dereference}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void enterDereference(SqlBaseParser.DereferenceContext ctx);

    /**
     * Exit a parse tree produced by the {@code dereference}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void exitDereference(SqlBaseParser.DereferenceContext ctx);

    /**
     * Enter a parse tree produced by the {@code cast}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void enterCast(SqlBaseParser.CastContext ctx);

    /**
     * Exit a parse tree produced by the {@code cast}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void exitCast(SqlBaseParser.CastContext ctx);

    /**
     * Enter a parse tree produced by the {@code constantDefault}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void enterConstantDefault(SqlBaseParser.ConstantDefaultContext ctx);

    /**
     * Exit a parse tree produced by the {@code constantDefault}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void exitConstantDefault(SqlBaseParser.ConstantDefaultContext ctx);

    /**
     * Enter a parse tree produced by the {@code extract}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void enterExtract(SqlBaseParser.ExtractContext ctx);

    /**
     * Exit a parse tree produced by the {@code extract}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void exitExtract(SqlBaseParser.ExtractContext ctx);

    /**
     * Enter a parse tree produced by the {@code parenthesizedExpression}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void enterParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx);

    /**
     * Exit a parse tree produced by the {@code parenthesizedExpression}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void exitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx);

    /**
     * Enter a parse tree produced by the {@code star}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void enterStar(SqlBaseParser.StarContext ctx);

    /**
     * Exit a parse tree produced by the {@code star}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void exitStar(SqlBaseParser.StarContext ctx);

    /**
     * Enter a parse tree produced by the {@code castOperatorExpression}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void enterCastOperatorExpression(SqlBaseParser.CastOperatorExpressionContext ctx);

    /**
     * Exit a parse tree produced by the {@code castOperatorExpression}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void exitCastOperatorExpression(SqlBaseParser.CastOperatorExpressionContext ctx);

    /**
     * Enter a parse tree produced by the {@code function}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void enterFunction(SqlBaseParser.FunctionContext ctx);

    /**
     * Exit a parse tree produced by the {@code function}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void exitFunction(SqlBaseParser.FunctionContext ctx);

    /**
     * Enter a parse tree produced by the {@code currentDateTimeFunction}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void enterCurrentDateTimeFunction(SqlBaseParser.CurrentDateTimeFunctionContext ctx);

    /**
     * Exit a parse tree produced by the {@code currentDateTimeFunction}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void exitCurrentDateTimeFunction(SqlBaseParser.CurrentDateTimeFunctionContext ctx);

    /**
     * Enter a parse tree produced by the {@code subqueryExpression}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void enterSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx);

    /**
     * Exit a parse tree produced by the {@code subqueryExpression}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void exitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx);

    /**
     * Enter a parse tree produced by the {@code case}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void enterCase(SqlBaseParser.CaseContext ctx);

    /**
     * Exit a parse tree produced by the {@code case}
     * labeled alternative in {@link SqlBaseParser#primaryExpression}.
     * @param ctx the parse tree
     */
    void exitCase(SqlBaseParser.CaseContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#builtinDateTimeFunction}.
     * @param ctx the parse tree
     */
    void enterBuiltinDateTimeFunction(SqlBaseParser.BuiltinDateTimeFunctionContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#builtinDateTimeFunction}.
     * @param ctx the parse tree
     */
    void exitBuiltinDateTimeFunction(SqlBaseParser.BuiltinDateTimeFunctionContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#castExpression}.
     * @param ctx the parse tree
     */
    void enterCastExpression(SqlBaseParser.CastExpressionContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#castExpression}.
     * @param ctx the parse tree
     */
    void exitCastExpression(SqlBaseParser.CastExpressionContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#castTemplate}.
     * @param ctx the parse tree
     */
    void enterCastTemplate(SqlBaseParser.CastTemplateContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#castTemplate}.
     * @param ctx the parse tree
     */
    void exitCastTemplate(SqlBaseParser.CastTemplateContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#convertTemplate}.
     * @param ctx the parse tree
     */
    void enterConvertTemplate(SqlBaseParser.ConvertTemplateContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#convertTemplate}.
     * @param ctx the parse tree
     */
    void exitConvertTemplate(SqlBaseParser.ConvertTemplateContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#extractExpression}.
     * @param ctx the parse tree
     */
    void enterExtractExpression(SqlBaseParser.ExtractExpressionContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#extractExpression}.
     * @param ctx the parse tree
     */
    void exitExtractExpression(SqlBaseParser.ExtractExpressionContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#extractTemplate}.
     * @param ctx the parse tree
     */
    void enterExtractTemplate(SqlBaseParser.ExtractTemplateContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#extractTemplate}.
     * @param ctx the parse tree
     */
    void exitExtractTemplate(SqlBaseParser.ExtractTemplateContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#functionExpression}.
     * @param ctx the parse tree
     */
    void enterFunctionExpression(SqlBaseParser.FunctionExpressionContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#functionExpression}.
     * @param ctx the parse tree
     */
    void exitFunctionExpression(SqlBaseParser.FunctionExpressionContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#functionTemplate}.
     * @param ctx the parse tree
     */
    void enterFunctionTemplate(SqlBaseParser.FunctionTemplateContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#functionTemplate}.
     * @param ctx the parse tree
     */
    void exitFunctionTemplate(SqlBaseParser.FunctionTemplateContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#functionName}.
     * @param ctx the parse tree
     */
    void enterFunctionName(SqlBaseParser.FunctionNameContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#functionName}.
     * @param ctx the parse tree
     */
    void exitFunctionName(SqlBaseParser.FunctionNameContext ctx);

    /**
     * Enter a parse tree produced by the {@code nullLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void enterNullLiteral(SqlBaseParser.NullLiteralContext ctx);

    /**
     * Exit a parse tree produced by the {@code nullLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void exitNullLiteral(SqlBaseParser.NullLiteralContext ctx);

    /**
     * Enter a parse tree produced by the {@code intervalLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void enterIntervalLiteral(SqlBaseParser.IntervalLiteralContext ctx);

    /**
     * Exit a parse tree produced by the {@code intervalLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void exitIntervalLiteral(SqlBaseParser.IntervalLiteralContext ctx);

    /**
     * Enter a parse tree produced by the {@code numericLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void enterNumericLiteral(SqlBaseParser.NumericLiteralContext ctx);

    /**
     * Exit a parse tree produced by the {@code numericLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void exitNumericLiteral(SqlBaseParser.NumericLiteralContext ctx);

    /**
     * Enter a parse tree produced by the {@code booleanLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void enterBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx);

    /**
     * Exit a parse tree produced by the {@code booleanLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void exitBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx);

    /**
     * Enter a parse tree produced by the {@code stringLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void enterStringLiteral(SqlBaseParser.StringLiteralContext ctx);

    /**
     * Exit a parse tree produced by the {@code stringLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void exitStringLiteral(SqlBaseParser.StringLiteralContext ctx);

    /**
     * Enter a parse tree produced by the {@code paramLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void enterParamLiteral(SqlBaseParser.ParamLiteralContext ctx);

    /**
     * Exit a parse tree produced by the {@code paramLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void exitParamLiteral(SqlBaseParser.ParamLiteralContext ctx);

    /**
     * Enter a parse tree produced by the {@code dateEscapedLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void enterDateEscapedLiteral(SqlBaseParser.DateEscapedLiteralContext ctx);

    /**
     * Exit a parse tree produced by the {@code dateEscapedLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void exitDateEscapedLiteral(SqlBaseParser.DateEscapedLiteralContext ctx);

    /**
     * Enter a parse tree produced by the {@code timeEscapedLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void enterTimeEscapedLiteral(SqlBaseParser.TimeEscapedLiteralContext ctx);

    /**
     * Exit a parse tree produced by the {@code timeEscapedLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void exitTimeEscapedLiteral(SqlBaseParser.TimeEscapedLiteralContext ctx);

    /**
     * Enter a parse tree produced by the {@code timestampEscapedLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void enterTimestampEscapedLiteral(SqlBaseParser.TimestampEscapedLiteralContext ctx);

    /**
     * Exit a parse tree produced by the {@code timestampEscapedLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void exitTimestampEscapedLiteral(SqlBaseParser.TimestampEscapedLiteralContext ctx);

    /**
     * Enter a parse tree produced by the {@code guidEscapedLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void enterGuidEscapedLiteral(SqlBaseParser.GuidEscapedLiteralContext ctx);

    /**
     * Exit a parse tree produced by the {@code guidEscapedLiteral}
     * labeled alternative in {@link SqlBaseParser#constant}.
     * @param ctx the parse tree
     */
    void exitGuidEscapedLiteral(SqlBaseParser.GuidEscapedLiteralContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#comparisonOperator}.
     * @param ctx the parse tree
     */
    void enterComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#comparisonOperator}.
     * @param ctx the parse tree
     */
    void exitComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#booleanValue}.
     * @param ctx the parse tree
     */
    void enterBooleanValue(SqlBaseParser.BooleanValueContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#booleanValue}.
     * @param ctx the parse tree
     */
    void exitBooleanValue(SqlBaseParser.BooleanValueContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#interval}.
     * @param ctx the parse tree
     */
    void enterInterval(SqlBaseParser.IntervalContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#interval}.
     * @param ctx the parse tree
     */
    void exitInterval(SqlBaseParser.IntervalContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#intervalField}.
     * @param ctx the parse tree
     */
    void enterIntervalField(SqlBaseParser.IntervalFieldContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#intervalField}.
     * @param ctx the parse tree
     */
    void exitIntervalField(SqlBaseParser.IntervalFieldContext ctx);

    /**
     * Enter a parse tree produced by the {@code primitiveDataType}
     * labeled alternative in {@link SqlBaseParser#dataType}.
     * @param ctx the parse tree
     */
    void enterPrimitiveDataType(SqlBaseParser.PrimitiveDataTypeContext ctx);

    /**
     * Exit a parse tree produced by the {@code primitiveDataType}
     * labeled alternative in {@link SqlBaseParser#dataType}.
     * @param ctx the parse tree
     */
    void exitPrimitiveDataType(SqlBaseParser.PrimitiveDataTypeContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#qualifiedName}.
     * @param ctx the parse tree
     */
    void enterQualifiedName(SqlBaseParser.QualifiedNameContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#qualifiedName}.
     * @param ctx the parse tree
     */
    void exitQualifiedName(SqlBaseParser.QualifiedNameContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#identifier}.
     * @param ctx the parse tree
     */
    void enterIdentifier(SqlBaseParser.IdentifierContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#identifier}.
     * @param ctx the parse tree
     */
    void exitIdentifier(SqlBaseParser.IdentifierContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#tableIdentifier}.
     * @param ctx the parse tree
     */
    void enterTableIdentifier(SqlBaseParser.TableIdentifierContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#tableIdentifier}.
     * @param ctx the parse tree
     */
    void exitTableIdentifier(SqlBaseParser.TableIdentifierContext ctx);

    /**
     * Enter a parse tree produced by the {@code quotedIdentifier}
     * labeled alternative in {@link SqlBaseParser#quoteIdentifier}.
     * @param ctx the parse tree
     */
    void enterQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx);

    /**
     * Exit a parse tree produced by the {@code quotedIdentifier}
     * labeled alternative in {@link SqlBaseParser#quoteIdentifier}.
     * @param ctx the parse tree
     */
    void exitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx);

    /**
     * Enter a parse tree produced by the {@code backQuotedIdentifier}
     * labeled alternative in {@link SqlBaseParser#quoteIdentifier}.
     * @param ctx the parse tree
     */
    void enterBackQuotedIdentifier(SqlBaseParser.BackQuotedIdentifierContext ctx);

    /**
     * Exit a parse tree produced by the {@code backQuotedIdentifier}
     * labeled alternative in {@link SqlBaseParser#quoteIdentifier}.
     * @param ctx the parse tree
     */
    void exitBackQuotedIdentifier(SqlBaseParser.BackQuotedIdentifierContext ctx);

    /**
     * Enter a parse tree produced by the {@code unquotedIdentifier}
     * labeled alternative in {@link SqlBaseParser#unquoteIdentifier}.
     * @param ctx the parse tree
     */
    void enterUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext ctx);

    /**
     * Exit a parse tree produced by the {@code unquotedIdentifier}
     * labeled alternative in {@link SqlBaseParser#unquoteIdentifier}.
     * @param ctx the parse tree
     */
    void exitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext ctx);

    /**
     * Enter a parse tree produced by the {@code digitIdentifier}
     * labeled alternative in {@link SqlBaseParser#unquoteIdentifier}.
     * @param ctx the parse tree
     */
    void enterDigitIdentifier(SqlBaseParser.DigitIdentifierContext ctx);

    /**
     * Exit a parse tree produced by the {@code digitIdentifier}
     * labeled alternative in {@link SqlBaseParser#unquoteIdentifier}.
     * @param ctx the parse tree
     */
    void exitDigitIdentifier(SqlBaseParser.DigitIdentifierContext ctx);

    /**
     * Enter a parse tree produced by the {@code decimalLiteral}
     * labeled alternative in {@link SqlBaseParser#number}.
     * @param ctx the parse tree
     */
    void enterDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx);

    /**
     * Exit a parse tree produced by the {@code decimalLiteral}
     * labeled alternative in {@link SqlBaseParser#number}.
     * @param ctx the parse tree
     */
    void exitDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx);

    /**
     * Enter a parse tree produced by the {@code integerLiteral}
     * labeled alternative in {@link SqlBaseParser#number}.
     * @param ctx the parse tree
     */
    void enterIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx);

    /**
     * Exit a parse tree produced by the {@code integerLiteral}
     * labeled alternative in {@link SqlBaseParser#number}.
     * @param ctx the parse tree
     */
    void exitIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#string}.
     * @param ctx the parse tree
     */
    void enterString(SqlBaseParser.StringContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#string}.
     * @param ctx the parse tree
     */
    void exitString(SqlBaseParser.StringContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#whenClause}.
     * @param ctx the parse tree
     */
    void enterWhenClause(SqlBaseParser.WhenClauseContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#whenClause}.
     * @param ctx the parse tree
     */
    void exitWhenClause(SqlBaseParser.WhenClauseContext ctx);

    /**
     * Enter a parse tree produced by {@link SqlBaseParser#nonReserved}.
     * @param ctx the parse tree
     */
    void enterNonReserved(SqlBaseParser.NonReservedContext ctx);

    /**
     * Exit a parse tree produced by {@link SqlBaseParser#nonReserved}.
     * @param ctx the parse tree
     */
    void exitNonReserved(SqlBaseParser.NonReservedContext ctx);
}
