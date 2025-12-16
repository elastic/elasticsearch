/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.Features.CROSS_CLUSTER;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.randomIndexPatterns;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.unquoteIndexPattern;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.without;
import static org.hamcrest.Matchers.containsString;

public class SubqueryTests extends AbstractStatementParserTests {

    /**
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnresolvedRelation[]
     */
    public void testIndexPatternWithSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var mainQueryIndexPattern = randomIndexPatterns();
        var subqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (FROM {})
            """, mainQueryIndexPattern, subqueryIndexPattern);

        LogicalPlan plan = statement(query);

        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        unresolvedRelation = as(subquery.plan(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(subqueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());
    }

    /**
     * Subqueries in the FROM command with all the processing commands in the main query.
     * All processing commands are supported in the main query when subqueries exist in the
     * FROM command. With an exception on FORK, the grammar or parser doesn't block FORK,
     * however nested FORK will error out in the analysis or logical planning phase. We are hoping
     * to lift this restriction in the future, so it is not blocked in the grammar.
     *
     * Rerank[test_reranker[KEYWORD],war and peace[KEYWORD],[?title AS title#45],?_score]
     * \_Sample[0.5[DOUBLE]]
     *   \_Completion[test_completion[KEYWORD],?prompt,?completion_output]
     *     \_ChangePoint[?count,?@timestamp,type{r}#39,pvalue{r}#40]
     *       \_Enrich[ANY,clientip_policy[KEYWORD],?client_ip,null,{},[?env]]
     *         \_LookupJoin[LEFT,[?n],[?n],false,null]
     *           |_MvExpand[?m,?m]
     *           | \_Rename[[?k AS l#29]]
     *           |   \_Keep[[?j]]
     *           |     \_Drop[[?i]]
     *           |       \_Limit[10[INTEGER],false]
     *           |         \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *           |           \_Grok[?h,Parser[pattern=%{WORD:word} %{NUMBER:number},
     *           grok=org.elasticsearch.grok.Grok@710201ab],[number{r}#22, word{r}#23]]
     *           |             \_Dissect[?g,Parser[pattern=%{b} %{c}, appendSeparator=,
     *           parser=org.elasticsearch.dissect.DissectParser@6bd8533a],[b{r}#16, c{r}#17]]
     *           |               \_InlineStats[]
     *           |                 \_Aggregate[[?f],[?MAX[?e] AS max_e#14, ?f]]
     *           |                   \_Aggregate[[?e],[?COUNT[*] AS cnt#11, ?e]]
     *           |                     \_Fork[[]]
     *           |                       |_Eval[[fork1[KEYWORD] AS _fork#7]]
     *           |                       | \_Filter[?c &gt; 100[INTEGER]]
     *           |                       |   \_Eval[[?a * 2[INTEGER] AS b#5]]
     *           |                       |     \_Filter[?a &gt; 10[INTEGER]]
     *           |                       |       \_UnionAll[[]]
     *           |                       |         |_UnresolvedRelation[]
     *           |                       |         \_Subquery[]
     *           |                       |           \_Filter[?a &lt; 100[INTEGER]]
     *           |                       |             \_UnresolvedRelation[]
     *           |                       \_Eval[[fork2[KEYWORD] AS _fork#7]]
     *           |                         \_Filter[?d &gt; 200[INTEGER]]
     *           |                           \_Eval[[?a * 2[INTEGER] AS b#5]]
     *           |                             \_Filter[?a &lt; 10[INTEGER]]
     *           |                               \_UnionAll[[]]
     *           |                                 |_UnresolvedRelation[]
     *           |                                 \_Subquery[]
     *           |                                   \_Filter[?a &lt; 100[INTEGER]]
     *           |                                     \_UnresolvedRelation[]
     *           \_UnresolvedRelation[lookup_index]
     */
    public void testSubqueryWithAllProcessingCommandsInMainquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        // remote cluster does not support COMPLETION or RERANK
        var mainQueryIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var subqueryIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var joinIndexPattern = "lookup_index"; // randomIndexPatterns may generate on as index pattern, it collides with the ON token
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (FROM {} | WHERE a < 100)
            | WHERE a > 10
            | EVAL b = a * 2
            | FORK (WHERE c < 100) (WHERE d > 200)
            | STATS cnt = COUNT(*) BY e
            | INLINE STATS max_e = MAX(e) BY f
            | DISSECT g "%{b} %{c}"
            | GROK h "%{WORD:word} %{NUMBER:number}"
            | SORT cnt desc
            | LIMIT 10
            | DROP i
            | KEEP j
            | RENAME k AS l
            | MV_EXPAND m
            | LOOKUP JOIN {} ON n
            | ENRICH clientip_policy ON client_ip WITH env
            | CHANGE_POINT count ON @timestamp AS type, pvalue
            | COMPLETION completion_output = prompt WITH { "inference_id" : "test_completion" }
            | SAMPLE 0.5
            | RERANK "war and peace" ON title WITH { "inference_id" : "test_reranker" }
            """, mainQueryIndexPattern, subqueryIndexPattern, joinIndexPattern);

        LogicalPlan plan = statement(query);
        Rerank rerank = as(plan, Rerank.class);
        Sample sample = as(rerank.child(), Sample.class);
        Completion completion = as(sample.child(), Completion.class);
        ChangePoint changePoint = as(completion.child(), ChangePoint.class);
        Enrich enrich = as(changePoint.child(), Enrich.class);
        LookupJoin lookupJoin = as(enrich.child(), LookupJoin.class);
        UnresolvedRelation joinRelation = as(lookupJoin.right(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(joinIndexPattern), joinRelation.indexPattern().indexPattern());
        MvExpand mvExpand = as(lookupJoin.left(), MvExpand.class);
        Rename rename = as(mvExpand.child(), Rename.class);
        Keep keep = as(rename.child(), Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Grok grok = as(orderBy.child(), Grok.class);
        Dissect dissect = as(grok.child(), Dissect.class);
        InlineStats inlineStats = as(dissect.child(), InlineStats.class);
        Aggregate aggregate = as(inlineStats.child(), Aggregate.class);
        aggregate = as(aggregate.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        List<LogicalPlan> forkChildren = fork.children();
        assertEquals(2, forkChildren.size());
        for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
            Filter forkFilter = as(forkEval.child(), Filter.class);
            Eval eval = as(forkFilter.child(), Eval.class);
            Filter filter = as(eval.child(), Filter.class);
            UnionAll unionAll = as(filter.child(), UnionAll.class);
            List<LogicalPlan> children = unionAll.children();
            assertEquals(2, children.size());
            // main query
            UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());
            // subquery
            Subquery subquery = as(children.get(1), Subquery.class);
            Filter subqueryFilter = as(subquery.plan(), Filter.class);
            LessThan lessThan = as(subqueryFilter.condition(), LessThan.class);
            Attribute left = as(lessThan.left(), Attribute.class);
            assertEquals("a", left.name());
            Literal right = as(lessThan.right(), Literal.class);
            assertEquals(100, right.value());
            UnresolvedRelation subqueryRelation = as(subqueryFilter.child(), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(subqueryIndexPattern), subqueryRelation.indexPattern().indexPattern());
        }
    }

    /**
     * Subqueries in the FROM command with all the processing commands in the subquery query.
     * The grammar allows all processing commands inside the subquery. With an exception on FORK,
     * the grammar or parser doesn't block FORK, however nested FORK will error out in the analysis
     * or logical planning phase. We are hoping to lift this restriction in the future, so it is not blocked
     * in the grammar.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_Rerank[test_reranker[KEYWORD],war and peace[KEYWORD],[?title AS title#30],?_score]
     *     \_Sample[0.5[DOUBLE]]
     *       \_Completion[test_completion[KEYWORD],?prompt,?completion_output]
     *         \_ChangePoint[?count,?@timestamp,type{r}#24,pvalue{r}#25]
     *           \_Enrich[ANY,clientip_policy[KEYWORD],?client_ip,null,{},[?env]]
     *             \_LookupJoin[LEFT,[?n],[?n],false,null]
     *               |_MvExpand[?m,?m]
     *               | \_Rename[[?k AS l#17]]
     *               |   \_Keep[[?j]]
     *               |     \_Drop[[?i]]
     *               |       \_Limit[10[INTEGER],false]
     *               |         \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *               |           \_Grok[?h,Parser[pattern=%{WORD:word} %{NUMBER:number},
     *               grok=org.elasticsearch.grok.Grok@2d54cab4],[number{r}#41, word{r}#42]]
     *               |             \_Dissect[?g,Parser[pattern=%{b} %{c}, appendSeparator=,
     *               parser=org.elasticsearch.dissect.DissectParser@5ca49d89],[b{r}#35, c{r}#36]]
     *               |               \_InlineStats[]
     *               |                 \_Aggregate[[?f],[?MAX[?e] AS max_e#10, ?f]]
     *               |                   \_Aggregate[[?e],[?COUNT[*] AS cnt#7, ?e]]
     *               |                     \_Fork[[]]
     *               |                       |_Eval[[fork1[KEYWORD] AS _fork#3]]
     *               |                       | \_Filter[?c &lt; 100[INTEGER]]
     *               |                       |   \_Eval[[?a * 2[INTEGER] AS b#34]]
     *               |                       |     \_Filter[?a &gt; 10[INTEGER]]
     *               |                       |       \_UnresolvedRelation[]
     *               |                       \_Eval[[fork2[KEYWORD] AS _fork#3]]
     *               |                         \_Filter[?d &gt; 200[INTEGER]]
     *               |                           \_Eval[[?a * 2[INTEGER] AS b#34]]
     *               |                             \_Filter[?a &gt; 10[INTEGER]]
     *               |                               \_UnresolvedRelation[]
     *               \_UnresolvedRelation[lookup_index]
     */
    public void testWithSubqueryWithProcessingCommandsInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var mainQueryIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var subqueryIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var joinIndexPattern = "lookup_index";
        String query = LoggerMessageFormat.format(null, """
             FROM {}, (FROM {}
                              | WHERE a > 10
                              | EVAL b = a * 2
                              | FORK (WHERE c < 100) (WHERE d > 200)
                              | STATS cnt = COUNT(*) BY e
                              | INLINE STATS max_e = MAX(e) BY f
                              | DISSECT g "%{b} %{c}"
                              | GROK h "%{WORD:word} %{NUMBER:number}"
                              | SORT cnt desc
                              | LIMIT 10
                              | DROP i
                              | KEEP j
                              | RENAME k AS l
                              | MV_EXPAND m
                              | LOOKUP JOIN {} ON n
                              | ENRICH clientip_policy ON client_ip WITH env
                              | CHANGE_POINT count ON @timestamp AS type, pvalue
                              | COMPLETION completion_output = prompt WITH { "inference_id" : "test_completion" }
                              | SAMPLE 0.5
                              | RERANK "war and peace" ON title WITH { "inference_id" : "test_reranker" })
            """, mainQueryIndexPattern, subqueryIndexPattern, joinIndexPattern);

        LogicalPlan plan = statement(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // main query
        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());
        // subquery
        Subquery subquery = as(children.get(1), Subquery.class);
        Rerank rerank = as(subquery.plan(), Rerank.class);
        Sample sample = as(rerank.child(), Sample.class);
        Completion completion = as(sample.child(), Completion.class);
        ChangePoint changePoint = as(completion.child(), ChangePoint.class);
        Enrich enrich = as(changePoint.child(), Enrich.class);
        LookupJoin lookupJoin = as(enrich.child(), LookupJoin.class);
        UnresolvedRelation joinRelation = as(lookupJoin.right(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(joinIndexPattern), joinRelation.indexPattern().indexPattern());
        MvExpand mvExpand = as(lookupJoin.left(), MvExpand.class);
        Rename rename = as(mvExpand.child(), Rename.class);
        Keep keep = as(rename.child(), Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Grok grok = as(orderBy.child(), Grok.class);
        Dissect dissect = as(grok.child(), Dissect.class);
        InlineStats inlineStats = as(dissect.child(), InlineStats.class);
        Aggregate aggregate = as(inlineStats.child(), Aggregate.class);
        aggregate = as(aggregate.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        List<LogicalPlan> forkChildren = fork.children();
        assertEquals(2, forkChildren.size());
        for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
            Filter forkFilter = as(forkEval.child(), Filter.class);
            Eval eval = as(forkFilter.child(), Eval.class);
            Filter filter = as(eval.child(), Filter.class);
            UnresolvedRelation subqueryRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(subqueryIndexPattern), subqueryRelation.indexPattern().indexPattern());
        }
    }

    /**
     * A combination of the two previous tests with processing commands in both the subquery and main query.
     * Plan string is skipped as it is too long, and it should be the combination of the above two tests..
     */
    public void testSubqueryWithProcessingCommandsInSubqueryAndMainquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var mainQueryIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var subqueryIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var joinIndexPattern = "lookup_index";
        String query = LoggerMessageFormat.format(null, """
             FROM {}, (FROM {}
                              | WHERE a > 10
                              | EVAL b = a * 2
                              | FORK (WHERE c < 100) (WHERE d > 200)
                              | STATS cnt = COUNT(*) BY e
                              | INLINE STATS max_e = MAX(e) BY f
                              | DISSECT g "%{b} %{c}"
                              | GROK h "%{WORD:word} %{NUMBER:number}"
                              | SORT cnt desc
                              | LIMIT 10
                              | DROP i
                              | KEEP j
                              | RENAME k AS l
                              | MV_EXPAND m
                              | LOOKUP JOIN {} ON n
                              | ENRICH clientip_policy ON client_ip WITH env
                              | CHANGE_POINT count ON @timestamp AS type, pvalue
                              | COMPLETION completion_output = prompt WITH { "inference_id" : "test_completion" }
                              | SAMPLE 0.5
                              | RERANK "war and peace" ON title WITH { "inference_id" : "test_reranker" })
             | WHERE a > 10
             | EVAL b = a * 2
             | FORK (WHERE c < 100) (WHERE d > 200)
             | STATS cnt = COUNT(*) BY e
             | INLINE STATS max_e = MAX(e) BY f
             | DISSECT g "%{b} %{c}"
             | GROK h "%{WORD:word} %{NUMBER:number}"
             | SORT cnt desc
             | LIMIT 10
             | DROP i
             | KEEP j
             | RENAME k AS l
             | MV_EXPAND m
             | LOOKUP JOIN {} ON n
             | ENRICH clientip_policy ON client_ip WITH env
             | CHANGE_POINT count ON @timestamp AS type, pvalue
             | COMPLETION completion_output = prompt WITH { "inference_id" : "test_completion" }
             | SAMPLE 0.5
             | RERANK "war and peace" ON title WITH { "inference_id" : "test_reranker" }
            """, mainQueryIndexPattern, subqueryIndexPattern, joinIndexPattern, joinIndexPattern);

        LogicalPlan plan = statement(query);
        Rerank rerank = as(plan, Rerank.class);
        Sample sample = as(rerank.child(), Sample.class);
        Completion completion = as(sample.child(), Completion.class);
        ChangePoint changePoint = as(completion.child(), ChangePoint.class);
        Enrich enrich = as(changePoint.child(), Enrich.class);
        LookupJoin lookupJoin = as(enrich.child(), LookupJoin.class);
        UnresolvedRelation joinRelation = as(lookupJoin.right(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(joinIndexPattern), joinRelation.indexPattern().indexPattern());
        MvExpand mvExpand = as(lookupJoin.left(), MvExpand.class);
        Rename rename = as(mvExpand.child(), Rename.class);
        Keep keep = as(rename.child(), Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Grok grok = as(orderBy.child(), Grok.class);
        Dissect dissect = as(grok.child(), Dissect.class);
        InlineStats inlineStats = as(dissect.child(), InlineStats.class);
        Aggregate aggregate = as(inlineStats.child(), Aggregate.class);
        aggregate = as(aggregate.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        List<LogicalPlan> forkChildren = fork.children();
        assertEquals(2, forkChildren.size());
        for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
            Filter forkFilter = as(forkEval.child(), Filter.class);
            Eval eval = as(forkFilter.child(), Eval.class);
            Filter filter = as(eval.child(), Filter.class);
            UnionAll unionAll = as(filter.child(), UnionAll.class);
            List<LogicalPlan> children = unionAll.children();
            assertEquals(2, children.size());
            // main query
            UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());
            // subquery
            Subquery subquery = as(children.get(1), Subquery.class);
            rerank = as(subquery.plan(), Rerank.class);
            sample = as(rerank.child(), Sample.class);
            completion = as(sample.child(), Completion.class);
            changePoint = as(completion.child(), ChangePoint.class);
            enrich = as(changePoint.child(), Enrich.class);
            lookupJoin = as(enrich.child(), LookupJoin.class);
            joinRelation = as(lookupJoin.right(), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(joinIndexPattern), joinRelation.indexPattern().indexPattern());
            mvExpand = as(lookupJoin.left(), MvExpand.class);
            rename = as(mvExpand.child(), Rename.class);
            keep = as(rename.child(), Keep.class);
            drop = as(keep.child(), Drop.class);
            limit = as(drop.child(), Limit.class);
            orderBy = as(limit.child(), OrderBy.class);
            grok = as(orderBy.child(), Grok.class);
            dissect = as(grok.child(), Dissect.class);
            inlineStats = as(dissect.child(), InlineStats.class);
            aggregate = as(inlineStats.child(), Aggregate.class);
            aggregate = as(aggregate.child(), Aggregate.class);
            fork = as(aggregate.child(), Fork.class);
            forkChildren = fork.children();
            assertEquals(2, forkChildren.size());
            for (Eval forkEvalSubquery : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
                forkFilter = as(forkEvalSubquery.child(), Filter.class);
                eval = as(forkFilter.child(), Eval.class);
                filter = as(eval.child(), Filter.class);
                UnresolvedRelation subqueryRelation = as(filter.child(), UnresolvedRelation.class);
                assertEquals(unquoteIndexPattern(subqueryIndexPattern), subqueryRelation.indexPattern().indexPattern());
            }
        }
    }

    /**
     * Verify there is no parsing error if the subquery ends with different modes.
     */
    public void testSubqueryEndsWithProcessingCommandsInDifferentMode() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        List<String> processingCommandInDifferentMode = List.of(
            "INLINE STATS max_e = MAX(e) BY f",  // inline mode, expression mode
            "DISSECT g \"%{b} %{c}\"",  // expression mode
            "LOOKUP JOIN index1 ON n", // join mode
            "ENRICH clientip_policy ON client_ip WITH env", // enrich mode
            "CHANGE_POINT count ON @timestamp AS type, pvalue", // change_point mode
            "FORK (WHERE c < 100) (WHERE d > 200)", // fork mode
            "MV_EXPAND m", // mv_expand mode
            "RENAME k AS l", // rename mode
            "DROP i" // project mode
        );
        var mainQueryIndexPattern = randomIndexPatterns();
        var subqueryIndexPattern = randomIndexPatterns();
        for (String processingCommand : processingCommandInDifferentMode) {
            String query = LoggerMessageFormat.format(null, """
                 FROM {}, (FROM {}
                                  | {})
                  | WHERE a > 10
                """, mainQueryIndexPattern, subqueryIndexPattern, processingCommand);

            LogicalPlan plan = statement(query);
            Filter filter = as(plan, Filter.class);
            UnionAll unionAll = as(filter.child(), UnionAll.class);
            List<LogicalPlan> children = unionAll.children();
            assertEquals(2, children.size());
            // main query
            UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());
            // subquery
            Subquery subquery = as(children.get(1), Subquery.class);
        }
    }

    /**
     * UnionAll[[]]
     * |_Subquery[]
     * | \_UnresolvedRelation[]
     * |_Subquery[]
     * | \_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnresolvedRelation[]
     */
    public void testSubqueryOnly() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var subqueryIndexPattern1 = randomIndexPatterns();
        var subqueryIndexPattern2 = randomIndexPatterns();
        var subqueryIndexPattern3 = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM (FROM {}), (FROM {}), (FROM {})
            """, subqueryIndexPattern1, subqueryIndexPattern2, subqueryIndexPattern3);

        LogicalPlan plan = statement(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());

        Subquery subquery = as(children.get(0), Subquery.class);
        UnresolvedRelation unresolvedRelation = as(subquery.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(subqueryIndexPattern1), unresolvedRelation.indexPattern().indexPattern());

        subquery = as(children.get(1), Subquery.class);
        unresolvedRelation = as(subquery.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(subqueryIndexPattern2), unresolvedRelation.indexPattern().indexPattern());

        subquery = as(children.get(2), Subquery.class);
        unresolvedRelation = as(subquery.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(subqueryIndexPattern3), unresolvedRelation.indexPattern().indexPattern());
    }

    /**
     * If the FROM command contains only one subquery, the subquery is merged into an index pattern.
     *
     * Keep[[?g]]
     * \_Drop[[?f]]
     *   \_Limit[10[INTEGER],false]
     *     \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *       \_Aggregate[[?e],[?COUNT[*] AS cnt#10, ?e]]
     *         \_Fork[[]]
     *           |_Eval[[fork1[KEYWORD] AS _fork#6]]
     *           | \_Filter[?c &lt; 100[INTEGER]]
     *           |   \_Eval[[?a * 2[INTEGER] AS b#4]]
     *           |     \_Filter[?a &gt; 10[INTEGER]]
     *           |       \_UnresolvedRelation[]
     *           \_Eval[[fork2[KEYWORD] AS _fork#6]]
     *             \_Filter[?d &gt; 200[INTEGER]]
     *               \_Eval[[?a * 2[INTEGER] AS b#4]]
     *                 \_Filter[?a &gt; 10[INTEGER]]
     *                   \_UnresolvedRelation[]
     */
    public void testSubqueryOnlyWithProcessingCommandInMainquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var subqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM (FROM {})
             | WHERE a > 10
             | EVAL b = a * 2
             | FORK (WHERE c < 100) (WHERE d > 200)
             | STATS cnt = COUNT(*) BY e
             | SORT cnt desc
             | LIMIT 10
             | DROP f
             | KEEP g
            """, subqueryIndexPattern);

        LogicalPlan plan = statement(query);
        Keep keep = as(plan, Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Aggregate aggregate = as(orderBy.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        List<LogicalPlan> forkChildren = fork.children();
        assertEquals(2, forkChildren.size());
        for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
            Filter forkFilter = as(forkEval.child(), Filter.class);
            Eval eval = as(forkFilter.child(), Eval.class);
            Filter filter = as(eval.child(), Filter.class);
            UnresolvedRelation unresolvedRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(subqueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());
        }
    }

    /**
     * Keep[[?g]]
     * \_Drop[[?f]]
     *   \_Limit[10[INTEGER],false]
     *     \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *       \_Aggregate[[?e],[?COUNT[*] AS cnt#7, ?e]]
     *         \_Fork[[]]
     *           |_Eval[[fork1[KEYWORD] AS _fork#3]]
     *           | \_Filter[?c &lt; 100[INTEGER]]
     *           |   \_Eval[[?a * 2[INTEGER] AS b#13]]
     *           |     \_Filter[?a &gt; 10[INTEGER]]
     *           |       \_UnresolvedRelation[]
     *           \_Eval[[fork2[KEYWORD] AS _fork#3]]
     *             \_Filter[?d &gt; 200[INTEGER]]
     *               \_Eval[[?a * 2[INTEGER] AS b#13]]
     *                 \_Filter[?a &gt; 10[INTEGER]]
     *                   \_UnresolvedRelation[]
     */
    public void testSubqueryOnlyWithProcessingCommandsInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var subqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM (FROM {}
                         | WHERE a > 10
                         | EVAL b = a * 2
                         | FORK (WHERE c < 100) (WHERE d > 200)
                         | STATS cnt = COUNT(*) BY e
                         | SORT cnt desc
                         | LIMIT 10
                         | DROP f
                         | KEEP g)
            """, subqueryIndexPattern);

        LogicalPlan plan = statement(query);
        Keep keep = as(plan, Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Aggregate aggregate = as(orderBy.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        List<LogicalPlan> forkChildren = fork.children();
        assertEquals(2, forkChildren.size());
        for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
            Filter forkFilter = as(forkEval.child(), Filter.class);
            Eval eval = as(forkFilter.child(), Eval.class);
            Filter filter = as(eval.child(), Filter.class);
            UnresolvedRelation subqueryRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(subqueryIndexPattern), subqueryRelation.indexPattern().indexPattern());
        }
    }

    /**
     * If the FROM command contains only a subquery, the subquery is merged into an index pattern.
     *
     * Keep[[?g]]
     * \_Drop[[?f]]
     *   \_Limit[10[INTEGER],false]
     *     \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *       \_Aggregate[[?e],[?COUNT[*] AS cnt#23, ?e]]
     *         \_Fork[[]]
     *           |_Eval[[fork1[KEYWORD] AS _fork#19]]
     *           | \_Filter[?c &lt; 100[INTEGER]]
     *           |   \_Eval[[?a * 2[INTEGER] AS b#17]]
     *           |     \_Filter[?a &gt; 10[INTEGER]]
     *           |       \_Keep[[?g]]
     *           |         \_Drop[[?f]]
     *           |           \_Limit[10[INTEGER],false]
     *           |             \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *           |               \_Aggregate[[?e],[?COUNT[*] AS cnt#7, ?e]]
     *           |                 \_Fork[[]]
     *           |                   |_Eval[[fork1[KEYWORD] AS _fork#3]]
     *           |                   | \_Filter[?c &lt; 100[INTEGER]]
     *           |                   |   \_Eval[[?a * 2[INTEGER] AS b#13]]
     *           |                   |     \_Filter[?a &gt; 10[INTEGER]]
     *           |                   |       \_UnresolvedRelation[]
     *           |                   \_Eval[[fork2[KEYWORD] AS _fork#3]]
     *           |                     \_Filter[?d &gt; 200[INTEGER]]
     *           |                       \_Eval[[?a * 2[INTEGER] AS b#13]]
     *           |                         \_Filter[?a &gt; 10[INTEGER]]
     *           |                           \_UnresolvedRelation[]
     *           \_Eval[[fork2[KEYWORD] AS _fork#19]]
     *             \_Filter[?d &gt; 200[INTEGER]]
     *               \_Eval[[?a * 2[INTEGER] AS b#17]]
     *                 \_Filter[?a &gt; 10[INTEGER]]
     *                   \_Keep[[?g]]
     *                     \_Drop[[?f]]
     *                       \_Limit[10[INTEGER],false]
     *                         \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *                           \_Aggregate[[?e],[?COUNT[*] AS cnt#7, ?e]]
     *                             \_Fork[[]]
     *                               |_Eval[[fork1[KEYWORD] AS _fork#3]]
     *                               | \_Filter[?c &lt; 100[INTEGER]]
     *                               |   \_Eval[[?a * 2[INTEGER] AS b#13]]
     *                               |     \_Filter[?a &gt; 10[INTEGER]]
     *                               |       \_UnresolvedRelation[]
     *                               \_Eval[[fork2[KEYWORD] AS _fork#3]]
     *                                 \_Filter[?d &gt; 200[INTEGER]]
     *                                   \_Eval[[?a * 2[INTEGER] AS b#13]]
     *                                     \_Filter[?a &gt; 10[INTEGER]]
     *                                       \_UnresolvedRelation[]
     */
    public void testSubqueryOnlyWithProcessingCommandsInSubqueryAndMainquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var subqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM (FROM {}
                         | WHERE a > 10
                         | EVAL b = a * 2
                         | FORK (WHERE c < 100) (WHERE d > 200)
                         | STATS cnt = COUNT(*) BY e
                         | SORT cnt desc
                         | LIMIT 10
                         | DROP f
                         | KEEP g)
              | WHERE a > 10
              | EVAL b = a * 2
              | FORK (WHERE c < 100) (WHERE d > 200)
              | STATS cnt = COUNT(*) BY e
              | SORT cnt desc
              | LIMIT 10
              | DROP f
              | KEEP g
            """, subqueryIndexPattern);

        LogicalPlan plan = statement(query);
        Keep keep = as(plan, Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Aggregate aggregate = as(orderBy.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        List<LogicalPlan> forkChildren = fork.children();
        assertEquals(2, forkChildren.size());
        for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
            Filter forkFilter = as(forkEval.child(), Filter.class);
            Eval eval = as(forkFilter.child(), Eval.class);
            Filter filter = as(eval.child(), Filter.class);
            Keep subqueryKeep = as(filter.child(), Keep.class);
            Drop subqueryDrop = as(subqueryKeep.child(), Drop.class);
            Limit subqueryLimit = as(subqueryDrop.child(), Limit.class);
            OrderBy subqueryOrderby = as(subqueryLimit.child(), OrderBy.class);
            Aggregate subqueryAggregate = as(subqueryOrderby.child(), Aggregate.class);
            Fork subqueryFork = as(subqueryAggregate.child(), Fork.class);
            List<LogicalPlan> subqueryForkChildren = subqueryFork.children();
            assertEquals(2, forkChildren.size());
            for (Eval subqueryForkEval : List.of(
                as(subqueryForkChildren.get(0), Eval.class),
                as(subqueryForkChildren.get(1), Eval.class)
            )) {
                Filter subqueryForkFilter = as(subqueryForkEval.child(), Filter.class);
                Eval subqueryEval = as(subqueryForkFilter.child(), Eval.class);
                Filter subqueryFilter = as(subqueryEval.child(), Filter.class);
                UnresolvedRelation subqueryRelation = as(subqueryFilter.child(), UnresolvedRelation.class);
                assertEquals(unquoteIndexPattern(subqueryIndexPattern), subqueryRelation.indexPattern().indexPattern());
            }
        }
    }

    /**
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * |_Subquery[]
     * | \_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnresolvedRelation[]
     */
    public void testMultipleMixedIndexPatternsAndSubqueries() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var indexPattern1 = randomIndexPatterns();
        var indexPattern2 = randomIndexPatterns();
        var indexPattern3 = randomIndexPatterns();
        var indexPattern4 = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM {}, (FROM {}), {}, (FROM {})
            """, indexPattern1, indexPattern2, indexPattern3, indexPattern4);

        LogicalPlan plan = statement(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(
            unquoteIndexPattern(indexPattern1) + "," + unquoteIndexPattern(indexPattern3),
            unresolvedRelation.indexPattern().indexPattern()
        );

        Subquery subquery1 = as(children.get(1), Subquery.class);
        unresolvedRelation = as(subquery1.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern2), unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery2 = as(children.get(2), Subquery.class);
        unresolvedRelation = as(subquery2.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern4), unresolvedRelation.indexPattern().indexPattern());
    }

    /**
     * Keep[[?g]]
     * \_Drop[[?f]]
     *   \_Limit[10[INTEGER],false]
     *     \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *       \_Aggregate[[?e],[?COUNT[*] AS cnt#25, ?e]]
     *         \_Fork[[]]
     *           |_Eval[[fork1[KEYWORD] AS _fork#21]]
     *           | \_Filter[?c &lt; 100[INTEGER]]
     *           |   \_LookupJoin[LEFT,[?c],[?c],true,null]
     *           |     |_Eval[[?a * 2[INTEGER] AS b#18]]
     *           |     | \_Filter[?a &gt; 10[INTEGER]]
     *           |     |   \_UnionAll[[]]
     *           |     |     |_UnresolvedRelation[]
     *           |     |     |_Subquery[]
     *           |     |     | \_Keep[[?g]]
     *           |     |     |   \_Drop[[?f]]
     *           |     |     |     \_Limit[10[INTEGER],false]
     *           |     |     |       \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *           |     |     |         \_Aggregate[[?e],[?COUNT[*] AS cnt#8, ?e]]
     *           |     |     |           \_Fork[[]]
     *           |     |     |             |_Eval[[fork1[KEYWORD] AS _fork#4]]
     *           |     |     |             | \_Filter[?c &lt; 100[INTEGER]]
     *           |     |     |             |   \_LookupJoin[LEFT,[?c],[?c],true,null]
     *           |     |     |             |     |_Eval[[?a * 2[INTEGER] AS b#14]]
     *           |     |     |             |     | \_Filter[?a &gt; 10[INTEGER]]
     *           |     |     |             |     |   \_UnresolvedRelation[]
     *           |     |     |             |     \_UnresolvedRelation[lookup_index]
     *           |     |     |             \_Eval[[fork2[KEYWORD] AS _fork#4]]
     *           |     |     |               \_Filter[?d &gt; 200[INTEGER]]
     *           |     |     |                 \_LookupJoin[LEFT,[?c],[?c],true,null]
     *           |     |     |                   |_Eval[[?a * 2[INTEGER] AS b#14]]
     *           |     |     |                   | \_Filter[?a &gt; 10[INTEGER]]
     *           |     |     |                   |   \_UnresolvedRelation[]
     *           |     |     |                   \_UnresolvedRelation[lookup_index]
     *           |     |     \_Subquery[]
     *           |     |       \_UnresolvedRelation[]
     *           |     \_UnresolvedRelation[lookup_index]
     *           \_Eval[[fork2[KEYWORD] AS _fork#21]]
     *             \_Filter[?d > 200[INTEGER]]
     *               \_LookupJoin[LEFT,[?c],[?c],true,null]
     *                 |_Eval[[?a * 2[INTEGER] AS b#18]]
     *                 | \_Filter[?a &gt; 10[INTEGER]]
     *                 |   \_UnionAll[[]]
     *                 |     |_UnresolvedRelation[]
     *                 |     |_Subquery[]
     *                 |     | \_Keep[[?g]]
     *                 |     |   \_Drop[[?f]]
     *                 |     |     \_Limit[10[INTEGER],false]
     *                 |     |       \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *                 |     |         \_Aggregate[[?e],[?COUNT[*] AS cnt#8, ?e]]
     *                 |     |           \_Fork[[]]
     *                 |     |             |_Eval[[fork1[KEYWORD] AS _fork#4]]
     *                 |     |             | \_Filter[?c &lt; 100[INTEGER]]
     *                 |     |             |   \_LookupJoin[LEFT,[?c],[?c],true,null]
     *                 |     |             |     |_Eval[[?a * 2[INTEGER] AS b#14]]
     *                 |     |             |     | \_Filter[?a &gt; 10[INTEGER]]
     *                 |     |             |     |   \_UnresolvedRelation[]
     *                 |     |             |     \_UnresolvedRelation[lookup_index]
     *                 |     |             \_Eval[[fork2[KEYWORD] AS _fork#4]]
     *                 |     |               \_Filter[?d &gt; 200[INTEGER]]
     *                 |     |                 \_LookupJoin[LEFT,[?c],[?c],true,null]
     *                 |     |                   |_Eval[[?a * 2[INTEGER] AS b#14]]
     *                 |     |                   | \_Filter[?a &gt; 10[INTEGER]]
     *                 |     |                   |   \_UnresolvedRelation[]
     *                 |     |                   \_UnresolvedRelation[lookup_index]
     *                 |     \_Subquery[]
     *                 |       \_UnresolvedRelation[]
     *                 \_UnresolvedRelation[lookup_index]
     */
    public void testMultipleSubqueriesWithProcessingCommands() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var mainIndexPattern1 = randomIndexPatterns();
        var mainIndexPattern2 = randomIndexPatterns();
        var subqueryIndexPattern1 = randomIndexPatterns();
        var subqueryIndexPattern2 = randomIndexPatterns();
        var joinIndexPattern = "lookup_index";
        var combinedIndexPattern = unquoteIndexPattern(mainIndexPattern1) + "," + unquoteIndexPattern(mainIndexPattern2);
        String query = LoggerMessageFormat.format(null, """
             FROM {}, (FROM {}
                              | WHERE a > 10
                              | EVAL b = a * 2
                              | LOOKUP JOIN {} ON c
                              | FORK (WHERE c < 100) (WHERE d > 200)
                              | STATS cnt = COUNT(*) BY e
                              | SORT cnt desc
                              | LIMIT 10
                              | DROP f
                              | KEEP g)
             , {}, (FROM {})
              | WHERE a > 10
              | EVAL b = a * 2
              | LOOKUP JOIN {} ON c
              | FORK (WHERE c < 100) (WHERE d > 200)
              | STATS cnt = COUNT(*) BY e
              | SORT cnt desc
              | LIMIT 10
              | DROP f
              | KEEP g
            """, mainIndexPattern1, subqueryIndexPattern1, joinIndexPattern, mainIndexPattern2, subqueryIndexPattern2, joinIndexPattern);

        LogicalPlan plan = statement(query);
        Keep keep = as(plan, Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Aggregate aggregate = as(orderBy.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        List<LogicalPlan> forkChildren = fork.children();
        assertEquals(2, forkChildren.size());
        for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
            Filter forkFilter = as(forkEval.child(), Filter.class);
            LookupJoin lookupJoin = as(forkFilter.child(), LookupJoin.class);
            Eval eval = as(lookupJoin.left(), Eval.class);
            UnresolvedRelation joinRelation = as(lookupJoin.right(), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(joinIndexPattern), joinRelation.indexPattern().indexPattern());
            Filter filter = as(eval.child(), Filter.class);

            UnionAll unionAll = as(filter.child(), UnionAll.class);
            List<LogicalPlan> children = unionAll.children();
            assertEquals(3, children.size());

            UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(combinedIndexPattern), unresolvedRelation.indexPattern().indexPattern());
            Subquery subquery1 = as(children.get(1), Subquery.class);
            Keep subqueryKeep = as(subquery1.plan(), Keep.class);
            Drop subqueryDrop = as(subqueryKeep.child(), Drop.class);
            Limit subqueryLimit = as(subqueryDrop.child(), Limit.class);
            OrderBy subqueryOrderby = as(subqueryLimit.child(), OrderBy.class);
            Aggregate subqueryAggregate = as(subqueryOrderby.child(), Aggregate.class);
            Fork subqueryFork = as(subqueryAggregate.child(), Fork.class);
            List<LogicalPlan> subqueryForkChildren = subqueryFork.children();
            assertEquals(2, forkChildren.size());
            for (Eval subqueryForkEval : List.of(
                as(subqueryForkChildren.get(0), Eval.class),
                as(subqueryForkChildren.get(1), Eval.class)
            )) {
                Filter subqueryForkFilter = as(subqueryForkEval.child(), Filter.class);
                LookupJoin subqueryLookupJoin = as(subqueryForkFilter.child(), LookupJoin.class);
                Eval subqueryEval = as(subqueryLookupJoin.left(), Eval.class);
                joinRelation = as(subqueryLookupJoin.right(), UnresolvedRelation.class);
                assertEquals(unquoteIndexPattern(joinIndexPattern), joinRelation.indexPattern().indexPattern());
                Filter subqueryFilter = as(subqueryEval.child(), Filter.class);
                unresolvedRelation = as(subqueryFilter.child(), UnresolvedRelation.class);
                assertEquals(unquoteIndexPattern(subqueryIndexPattern1), unresolvedRelation.indexPattern().indexPattern());
            }

            Subquery subquery2 = as(children.get(2), Subquery.class);
            unresolvedRelation = as(subquery2.plan(), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(subqueryIndexPattern2), unresolvedRelation.indexPattern().indexPattern());
        }
    }

    /**
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnionAll[[]]
     *     |_UnresolvedRelation[]
     *     \_Subquery[]
     *       \_UnionAll[[]]
     *         |_UnresolvedRelation[]
     *         \_Subquery[]
     *           \_UnresolvedRelation[]
     */
    public void testSimpleNestedSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var indexPattern1 = randomIndexPatterns();
        var indexPattern2 = randomIndexPatterns();
        var indexPattern3 = randomIndexPatterns();
        var indexPattern4 = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM {}, (FROM {}, (FROM {}, (FROM {})))
            """, indexPattern1, indexPattern2, indexPattern3, indexPattern4);

        LogicalPlan plan = statement(query);

        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern1), unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery1 = as(children.get(1), Subquery.class);
        unionAll = as(subquery1.plan(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());

        unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern2), unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery2 = as(children.get(1), Subquery.class);
        unionAll = as(subquery2.plan(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());

        unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern3), unresolvedRelation.indexPattern().indexPattern());
        Subquery subquery3 = as(children.get(1), Subquery.class);
        unresolvedRelation = as(subquery3.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern4), unresolvedRelation.indexPattern().indexPattern());
    }

    /**
     * LogicalPlanBuilder does not flatten nested subqueries with processing commands,
     * the structure of the nested subqueries s preserved in the parsed plan.
     *
     * Limit[10[INTEGER],false]
     * \_UnionAll[[]]
     *   |_UnresolvedRelation[]
     *   \_Subquery[]
     *     \_Aggregate[[?e],[?COUNT[*] AS cnt#7, ?e]]
     *       \_UnionAll[[]]
     *         |_UnresolvedRelation[]
     *         \_Subquery[]
     *           \_Eval[[?a * 2[INTEGER] AS b#4]]
     *             \_UnionAll[[]]
     *               |_UnresolvedRelation[]
     *               \_Subquery[]
     *                 \_Filter[?a > 10[INTEGER]]
     *                   \_UnresolvedRelation[]
     */
    public void testNestedSubqueryWithProcessingCommands() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var indexPattern1 = randomIndexPatterns();
        var indexPattern2 = randomIndexPatterns();
        var indexPattern3 = randomIndexPatterns();
        var indexPattern4 = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM {}, (FROM {}, (FROM {}, (FROM {}
                                                                | WHERE a > 10)
                                               | EVAL b = a * 2)
                              |STATS cnt = COUNT(*) BY e)
            | LIMIT 10
            """, indexPattern1, indexPattern2, indexPattern3, indexPattern4);

        LogicalPlan plan = statement(query);
        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern1), unresolvedRelation.indexPattern().indexPattern());
        Subquery subquery1 = as(children.get(1), Subquery.class);
        Aggregate aggregate = as(subquery1.plan(), Aggregate.class);
        unionAll = as(aggregate.child(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());
        unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern2), unresolvedRelation.indexPattern().indexPattern());
        Subquery subquery2 = as(children.get(1), Subquery.class);
        Eval eval = as(subquery2.plan(), Eval.class);
        unionAll = as(eval.child(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());
        unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern3), unresolvedRelation.indexPattern().indexPattern());
        Subquery subquery3 = as(children.get(1), Subquery.class);
        Filter filter = as(subquery3.plan(), Filter.class);
        unresolvedRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern4), unresolvedRelation.indexPattern().indexPattern());
    }

    /**
     * The medatada options from the main query are not propagated into subqueries.
     *
     * Aggregate[[?a],[?COUNT[*] AS cnt#6, ?a]]
     * \_UnionAll[[]]
     *   |_UnresolvedRelation[]
     *   \_Subquery[]
     *     \_Filter[?a &gt; 10[INTEGER]]
     *       \_UnionAll[[]]
     *         |_UnresolvedRelation[]
     *         \_Subquery[]
     *           \_UnresolvedRelation[]
     */
    public void testSubqueriesWithMetadada() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var indexPattern1 = randomIndexPatterns();
        var indexPattern2 = randomIndexPatterns();
        var indexPattern3 = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM {}, (FROM {}, (FROM {}) metadata _score | WHERE a > 10) metadata _index
             | STATS cnt = COUNT(*) BY a
            """, indexPattern1, indexPattern2, indexPattern3);

        LogicalPlan plan = statement(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        UnionAll unionAll = as(aggregate.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // main query
        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern1), mainRelation.indexPattern().indexPattern());
        List<Attribute> metadata = mainRelation.metadataFields();
        assertEquals(1, metadata.size());
        MetadataAttribute metadataAttribute = as(metadata.get(0), MetadataAttribute.class);
        assertEquals("_index", metadataAttribute.name());
        // subquery1
        Subquery subquery = as(children.get(1), Subquery.class);
        Filter filter = as(subquery.plan(), Filter.class);
        unionAll = as(filter.child(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());
        UnresolvedRelation subqueryRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern2), subqueryRelation.indexPattern().indexPattern());
        metadata = subqueryRelation.metadataFields();
        assertEquals(1, metadata.size());
        metadataAttribute = as(metadata.get(0), MetadataAttribute.class);
        assertEquals("_score", metadataAttribute.name());
        // subquery2
        subquery = as(children.get(1), Subquery.class);
        subqueryRelation = as(subquery.plan(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern3), subqueryRelation.indexPattern().indexPattern());
        metadata = subqueryRelation.metadataFields();
        assertEquals(0, metadata.size());
    }

    /**
     * Aggregate[[?a],[?COUNT[*] AS cnt#4, ?a]]
     * \_UnionAll[[]]
     *   |_UnresolvedRelation[]
     *   \_Subquery[]
     *     \_Filter[?a &gt; 10[INTEGER]]
     *       \_UnresolvedRelation[]
     */
    public void testSubqueryWithRemoteCluster() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var mainRemoteIndexPattern = randomIndexPatterns(CROSS_CLUSTER);
        var mainIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var combinedMainIndexPattern = unquoteIndexPattern(mainRemoteIndexPattern) + "," + unquoteIndexPattern(mainIndexPattern);
        var subqueryRemoteIndexPattern = randomIndexPatterns(CROSS_CLUSTER);
        var subqueryIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var combinedSubqueryIndexPattern = unquoteIndexPattern(subqueryRemoteIndexPattern)
            + ","
            + unquoteIndexPattern(subqueryIndexPattern);
        String query = LoggerMessageFormat.format(null, """
             FROM {}, {}, (FROM {}, {} | WHERE a > 10)
             | STATS cnt = COUNT(*) BY a
            """, mainRemoteIndexPattern, mainIndexPattern, subqueryRemoteIndexPattern, subqueryIndexPattern);

        LogicalPlan plan = statement(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        UnionAll unionAll = as(aggregate.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // main query
        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(combinedMainIndexPattern, mainRelation.indexPattern().indexPattern());
        // subquery
        Subquery subquery = as(children.get(1), Subquery.class);
        Filter filter = as(subquery.plan(), Filter.class);
        UnresolvedRelation unresolvedRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals(combinedSubqueryIndexPattern, unresolvedRelation.indexPattern().indexPattern());
    }

    public void testTimeSeriesWithSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var mainIndexPattern = randomIndexPatterns();
        var subqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             TS index1, (FROM index2)
            """, mainIndexPattern, subqueryIndexPattern);

        expectThrows(
            ParsingException.class,
            containsString("line 1:2: Subqueries are not supported in TS command"),
            () -> statement(query)
        );
    }
}
