/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.FIVE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.FOUR;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.ONE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.SIX;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.THREE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOrEqualOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomMinimumVersion;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.rlike;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.wildcardLike;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin.firstSubPlan;
import static org.elasticsearch.xpack.esql.session.EsqlSession.newMainPlan;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class PushDownAndCombineFiltersTests extends AbstractLogicalPlanOptimizerTests {

    private final LogicalOptimizerContext optimizerContext = new LogicalOptimizerContext(null, FoldContext.small(), randomMinimumVersion());

    public void testCombineFilters() {
        EsRelation relation = relation();
        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);

        Filter fa = new Filter(EMPTY, relation, conditionA);
        Filter fb = new Filter(EMPTY, fa, conditionB);

        assertEquals(
            new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)),
            new PushDownAndCombineFilters().apply(fb, optimizerContext)
        );
    }

    public void testCombineFiltersLikeRLike() {
        EsRelation relation = relation();
        RLike conditionA = rlike(getFieldAttribute("a"), "foo");
        WildcardLike conditionB = wildcardLike(getFieldAttribute("b"), "bar");

        Filter fa = new Filter(EMPTY, relation, conditionA);
        Filter fb = new Filter(EMPTY, fa, conditionB);

        assertEquals(
            new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)),
            new PushDownAndCombineFilters().apply(fb, optimizerContext)
        );
    }

    public void testPushDownFilter() {
        EsRelation relation = relation();
        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);

        Filter fa = new Filter(EMPTY, relation, conditionA);
        List<FieldAttribute> projections = singletonList(getFieldAttribute("b"));
        EsqlProject keep = new EsqlProject(EMPTY, fa, projections);
        Filter fb = new Filter(EMPTY, keep, conditionB);

        Filter combinedFilter = new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB));
        assertEquals(new EsqlProject(EMPTY, combinedFilter, projections), new PushDownAndCombineFilters().apply(fb, optimizerContext));
    }

    public void testPushDownFilterPastRenamingProject() {
        FieldAttribute a = getFieldAttribute("a");
        FieldAttribute b = getFieldAttribute("b");
        EsRelation relation = relation(List.of(a, b));

        Alias aRenamed = new Alias(EMPTY, "a_renamed", a);
        Alias aRenamedTwice = new Alias(EMPTY, "a_renamed_twice", aRenamed.toAttribute());
        Alias bRenamed = new Alias(EMPTY, "b_renamed", b);

        Project project = new Project(EMPTY, relation, List.of(aRenamed, aRenamedTwice, bRenamed));

        GreaterThan aRenamedTwiceGreaterThanOne = greaterThanOf(aRenamedTwice.toAttribute(), ONE);
        LessThan bRenamedLessThanTwo = lessThanOf(bRenamed.toAttribute(), TWO);
        Filter filter = new Filter(EMPTY, project, Predicates.combineAnd(List.of(aRenamedTwiceGreaterThanOne, bRenamedLessThanTwo)));

        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter, optimizerContext);

        Project optimizedProject = as(optimized, Project.class);
        assertEquals(optimizedProject.projections(), project.projections());
        Filter optimizedFilter = as(optimizedProject.child(), Filter.class);
        assertEquals(optimizedFilter.condition(), Predicates.combineAnd(List.of(greaterThanOf(a, ONE), lessThanOf(b, TWO))));
        EsRelation optimizedRelation = as(optimizedFilter.child(), EsRelation.class);
        assertEquals(optimizedRelation, relation);
    }

    // ... | eval a_renamed = a, a_renamed_twice = a_renamed, a_squared = pow(a, 2)
    // | where a_renamed > 1 and a_renamed_twice < 2 and a_squared < 4
    // ->
    // ... | where a > 1 and a < 2 | eval a_renamed = a, a_renamed_twice = a_renamed, non_pushable = pow(a, 2) | where a_squared < 4
    public void testPushDownFilterOnAliasInEval() {
        FieldAttribute a = getFieldAttribute("a");
        FieldAttribute b = getFieldAttribute("b");
        EsRelation relation = relation(List.of(a, b));

        Alias aRenamed = new Alias(EMPTY, "a_renamed", a);
        Alias aRenamedTwice = new Alias(EMPTY, "a_renamed_twice", aRenamed.toAttribute());
        Alias bRenamed = new Alias(EMPTY, "b_renamed", b);
        Alias aSquared = new Alias(EMPTY, "a_squared", new Pow(EMPTY, a, TWO));
        Eval eval = new Eval(EMPTY, relation, List.of(aRenamed, aRenamedTwice, aSquared, bRenamed));

        // We'll construct a Filter after the Eval that has conditions that can or cannot be pushed before the Eval.
        List<Expression> pushableConditionsBefore = List.of(
            greaterThanOf(a.toAttribute(), TWO),
            greaterThanOf(aRenamed.toAttribute(), ONE),
            lessThanOf(aRenamedTwice.toAttribute(), TWO),
            lessThanOf(aRenamedTwice.toAttribute(), bRenamed.toAttribute())
        );
        List<Expression> pushableConditionsAfter = List.of(
            greaterThanOf(a.toAttribute(), TWO),
            greaterThanOf(a.toAttribute(), ONE),
            lessThanOf(a.toAttribute(), TWO),
            lessThanOf(a.toAttribute(), b.toAttribute())
        );
        List<Expression> nonPushableConditions = List.of(
            lessThanOf(aSquared.toAttribute(), FOUR),
            greaterThanOf(aRenamedTwice.toAttribute(), aSquared.toAttribute())
        );

        // Try different combinations of pushable and non-pushable conditions in the filter while also randomizing their order a bit.
        for (int numPushable = 0; numPushable <= pushableConditionsBefore.size(); numPushable++) {
            for (int numNonPushable = 0; numNonPushable <= nonPushableConditions.size(); numNonPushable++) {
                if (numPushable == 0 && numNonPushable == 0) {
                    continue;
                }

                List<Expression> conditions = new ArrayList<>();

                int pushableIndex = 0, nonPushableIndex = 0;
                // Loop and add either a pushable or non-pushable condition to the filter.
                boolean addPushable;
                while (pushableIndex < numPushable || nonPushableIndex < numNonPushable) {
                    if (pushableIndex == numPushable) {
                        addPushable = false;
                    } else if (nonPushableIndex == numNonPushable) {
                        addPushable = true;
                    } else {
                        addPushable = randomBoolean();
                    }

                    if (addPushable) {
                        conditions.add(pushableConditionsBefore.get(pushableIndex++));
                    } else {
                        conditions.add(nonPushableConditions.get(nonPushableIndex++));
                    }
                }

                Filter filter = new Filter(EMPTY, eval, Predicates.combineAnd(conditions));

                LogicalPlan plan = new PushDownAndCombineFilters().apply(filter, optimizerContext);

                if (numNonPushable > 0) {
                    Filter optimizedFilter = as(plan, Filter.class);
                    assertEquals(optimizedFilter.condition(), Predicates.combineAnd(nonPushableConditions.subList(0, numNonPushable)));
                    plan = optimizedFilter.child();
                }
                Eval optimizedEval = as(plan, Eval.class);
                assertEquals(optimizedEval.fields(), eval.fields());
                plan = optimizedEval.child();
                if (numPushable > 0) {
                    Filter pushedFilter = as(plan, Filter.class);
                    assertEquals(pushedFilter.condition(), Predicates.combineAnd(pushableConditionsAfter.subList(0, numPushable)));
                    plan = pushedFilter.child();
                }
                EsRelation optimizedRelation = as(plan, EsRelation.class);
                assertEquals(optimizedRelation, relation);
            }
        }
    }

    public void testPushDownLikeRlikeFilter() {
        EsRelation relation = relation();
        RLike conditionA = rlike(getFieldAttribute("a"), "foo");
        WildcardLike conditionB = wildcardLike(getFieldAttribute("b"), "bar");

        Filter fa = new Filter(EMPTY, relation, conditionA);
        List<FieldAttribute> projections = singletonList(getFieldAttribute("b"));
        EsqlProject keep = new EsqlProject(EMPTY, fa, projections);
        Filter fb = new Filter(EMPTY, keep, conditionB);

        Filter combinedFilter = new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB));
        assertEquals(new EsqlProject(EMPTY, combinedFilter, projections), new PushDownAndCombineFilters().apply(fb, optimizerContext));
    }

    // from ... | where a > 1 | stats count(1) by b | where count(1) >= 3 and b < 2
    // => ... | where a > 1 and b < 2 | stats count(1) by b | where count(1) >= 3
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/115311")
    public void testSelectivelyPushDownFilterPastFunctionAgg() {
        EsRelation relation = relation();
        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);
        GreaterThanOrEqual aggregateCondition = greaterThanOrEqualOf(new Count(EMPTY, ONE), THREE);

        Filter fa = new Filter(EMPTY, relation, conditionA);
        // invalid aggregate but that's fine cause its properties are not used by this rule
        Aggregate aggregate = new Aggregate(EMPTY, fa, singletonList(getFieldAttribute("b")), emptyList());
        Filter fb = new Filter(EMPTY, aggregate, new And(EMPTY, aggregateCondition, conditionB));

        // expected
        Filter expected = new Filter(
            EMPTY,
            new Aggregate(
                EMPTY,
                new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)),
                singletonList(getFieldAttribute("b")),
                emptyList()
            ),
            aggregateCondition
        );
        assertEquals(expected, new PushDownAndCombineFilters().apply(fb, optimizerContext));
    }

    // from ... | where a > 1 | COMPLETION completion = "some prompt" WITH { "inferenceId' : "inferenceId" } | where b < 2 and
    // match(completion, some text)
    // => ... | where a > 1 AND b < 2| COMPLETION completion = "some prompt" WITH { "inferenceId' : "inferenceId" } | where
    // match(completion,
    // some text)
    public void testPushDownFilterPastCompletion() {
        FieldAttribute a = getFieldAttribute("a");
        FieldAttribute b = getFieldAttribute("b");
        EsRelation relation = relation(List.of(a, b));

        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        Filter filterA = new Filter(EMPTY, relation, conditionA);

        Completion completion = completion(filterA);

        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);
        Match conditionCompletion = new Match(
            EMPTY,
            completion.targetField(),
            randomLiteral(DataType.TEXT),
            mock(Expression.class),
            mock(QueryBuilder.class)
        );
        Filter filterB = new Filter(EMPTY, completion, new And(EMPTY, conditionB, conditionCompletion));

        LogicalPlan expectedOptimizedPlan = new Filter(
            EMPTY,
            new Completion(
                EMPTY,
                new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)),
                completion.inferenceId(),
                completion.prompt(),
                completion.targetField()
            ),
            conditionCompletion
        );

        assertEquals(expectedOptimizedPlan, new PushDownAndCombineFilters().apply(filterB, optimizerContext));
    }

    // from ... | where a > 1 | RERANK "query" ON title WITH { "inference_id" : "inferenceId" } | where b < 2 and _score > 1
    // => ... | where a > 1 AND b < 2| RERANK "query" ON title WITH { "inference_id" : "inferenceId" } | where _score > 1
    public void testPushDownFilterPastRerank() {
        FieldAttribute a = getFieldAttribute("a");
        FieldAttribute b = getFieldAttribute("b");
        EsRelation relation = relation(List.of(a, b));

        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        Filter filterA = new Filter(EMPTY, relation, conditionA);

        Rerank rerank = rerank(filterA);

        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);
        GreaterThan scoreCondition = greaterThanOf(rerank.scoreAttribute(), ONE);

        Filter filterB = new Filter(EMPTY, rerank, new And(EMPTY, conditionB, scoreCondition));

        LogicalPlan expectedOptimizedPlan = new Filter(
            EMPTY,
            new Rerank(
                EMPTY,
                new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)),
                rerank.inferenceId(),
                rerank.queryText(),
                rerank.rerankFields(),
                rerank.scoreAttribute()
            ),
            scoreCondition
        );

        assertEquals(expectedOptimizedPlan, new PushDownAndCombineFilters().apply(filterB, optimizerContext));
    }

    private static Completion completion(LogicalPlan child) {
        return new Completion(
            EMPTY,
            child,
            randomLiteral(DataType.KEYWORD),
            randomLiteral(randomBoolean() ? DataType.TEXT : DataType.KEYWORD),
            referenceAttribute(randomIdentifier(), DataType.KEYWORD)
        );
    }

    private static Rerank rerank(LogicalPlan child) {
        return new Rerank(
            EMPTY,
            child,
            randomLiteral(DataType.KEYWORD),
            randomLiteral(randomBoolean() ? DataType.TEXT : DataType.KEYWORD),
            randomList(1, 10, () -> new Alias(EMPTY, randomIdentifier(), randomLiteral(DataType.KEYWORD))),
            referenceAttribute(randomBoolean() ? MetadataAttribute.SCORE : randomIdentifier(), DataType.DOUBLE)
        );
    }

    private static EsRelation relation() {
        return relation(List.of());
    }

    private static EsRelation relation(List<Attribute> fieldAttributes) {
        return new EsRelation(EMPTY, randomIdentifier(), randomFrom(IndexMode.values()), Map.of(), fieldAttributes);
    }

    public void testPushDownFilterPastLeftJoinWithPushable() {
        Join join = createLeftJoinOnFields();
        EsRelation left = (EsRelation) join.left();
        FieldAttribute c = (FieldAttribute) join.right().output().get(0);

        // Pushable filter
        Expression pushableCondition = greaterThanOf(c, ONE);
        Filter filter = new Filter(EMPTY, join, pushableCondition);
        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter, optimizerContext);
        // The filter should still be on top
        Filter topFilter = as(optimized, Filter.class);
        assertEquals(pushableCondition, topFilter.condition());
        Join optimizedJoin = as(topFilter.child(), Join.class);
        assertEquals(left, optimizedJoin.left());
        Filter rightFilter = as(optimizedJoin.right(), Filter.class);
        assertEquals(pushableCondition, rightFilter.condition());
    }

    public void testPushDownFilterPastLeftJoinWithExistingFilter() {
        Join join = createLeftJoinOnFields();
        EsRelation left = (EsRelation) join.left();
        FieldAttribute c = (FieldAttribute) join.right().output().get(0);

        // Existing filter on the right side
        Expression existingCondition = lessThanOf(c, FIVE);
        Filter existingFilter = new Filter(EMPTY, join.right(), existingCondition);
        join = (Join) join.replaceRight(existingFilter);

        // Pushable filter
        Expression pushableCondition = greaterThanOf(c, ONE);
        Filter filter = new Filter(EMPTY, join, pushableCondition);
        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter, optimizerContext);
        // The filter should still be on top
        Filter topFilter = as(optimized, Filter.class);
        assertEquals(pushableCondition, topFilter.condition());
        Join optimizedJoin = as(topFilter.child(), Join.class);
        assertEquals(left, optimizedJoin.left());
        Filter rightFilter = as(optimizedJoin.right(), Filter.class);

        // The new condition should be merged with the existing one
        Expression combinedCondition = new And(EMPTY, existingCondition, pushableCondition);
        assertEquals(combinedCondition, rightFilter.condition());

        // try to apply the filter again, the plan should not change
        // this verifies that the rule is idempotent
        // and we will not get in an infinite loop pushing the same filter over and over
        optimized = new PushDownAndCombineFilters().apply(optimized, optimizerContext);

        topFilter = as(optimized, Filter.class);
        assertEquals(pushableCondition, topFilter.condition());
        optimizedJoin = as(topFilter.child(), Join.class);
        assertEquals(left, optimizedJoin.left());
        rightFilter = as(optimizedJoin.right(), Filter.class);

        // The new condition should be merged with the existing one
        assertEquals(combinedCondition, rightFilter.condition());

    }

    public void testDoNotPushDownExistingFilterAgain() {
        Join join = createLeftJoinOnFields();
        EsRelation left = (EsRelation) join.left();
        FieldAttribute c = (FieldAttribute) join.right().output().get(0);

        // Existing filter on the right side
        Expression existingCondition = greaterThanOf(c, ONE);
        Filter existingFilter = new Filter(EMPTY, join.right(), existingCondition);
        join = (Join) join.replaceRight(existingFilter);

        // A filter on top with the same condition
        Filter filter = new Filter(EMPTY, join, existingCondition);
        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter, optimizerContext);

        // The filter should still be on top
        Filter topFilter = as(optimized, Filter.class);
        assertEquals(existingCondition, topFilter.condition());

        Join optimizedJoin = as(topFilter.child(), Join.class);
        assertEquals(left, optimizedJoin.left());

        // The right side should be the original filter, unchanged.
        Filter rightFilter = as(optimizedJoin.right(), Filter.class);
        assertEquals(existingFilter, rightFilter);
        assertEquals(existingCondition, rightFilter.condition());
    }

    public void testPushDownFilterPastLeftJoinWithExistingFilterCalledTwice() {
        Join join = createLeftJoinOnFields();
        EsRelation left = (EsRelation) join.left();
        FieldAttribute c = (FieldAttribute) join.right().output().get(0);

        // Pushable filter
        Expression pushableCondition = greaterThanOf(c, ONE);
        Filter filter = new Filter(EMPTY, join, pushableCondition);

        // First optimization
        LogicalPlan optimizedOnce = new PushDownAndCombineFilters().apply(filter, optimizerContext);

        // Second optimization
        LogicalPlan optimizedTwice = new PushDownAndCombineFilters().apply(optimizedOnce, optimizerContext);

        // The filter should still be on top
        Filter topFilter = as(optimizedTwice, Filter.class);
        assertEquals(pushableCondition, topFilter.condition());

        Join optimizedJoin = as(topFilter.child(), Join.class);
        assertEquals(left, optimizedJoin.left());

        // The right side should have the filter, but not duplicated.
        Filter rightFilter = as(optimizedJoin.right(), Filter.class);
        assertEquals(pushableCondition, rightFilter.condition());
    }

    public void testPushDownFilterPastLeftJoinWithNonPushable() {
        Join join = createLeftJoinOnFields();
        FieldAttribute c = (FieldAttribute) join.right().output().get(0);

        // Non-pushable filter
        Expression nonPushableCondition = new IsNull(EMPTY, c);
        Filter filter = new Filter(EMPTY, join, nonPushableCondition);
        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter, optimizerContext);
        // No optimization should be applied, the plan should be the same
        assertEquals(filter, optimized);
        // And the join inside should not have candidate filters
        Join innerJoin = as(as(optimized, Filter.class).child(), Join.class);
        assertFalse(innerJoin.right() instanceof Filter);
    }

    public void testPushDownFilterPastLeftJoinWithPartiallyPushableAnd() {
        Join join = createLeftJoinOnFields();
        EsRelation left = (EsRelation) join.left();
        FieldAttribute c = (FieldAttribute) join.right().output().get(0);

        Expression pushableCondition = greaterThanOf(c, ONE);
        Expression nonPushableCondition = new IsNull(EMPTY, c);

        // Partially pushable filter
        Expression partialCondition = new And(EMPTY, pushableCondition, nonPushableCondition);
        Filter filter = new Filter(EMPTY, join, partialCondition);
        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter, optimizerContext);
        Filter topFilter = as(optimized, Filter.class);
        // The top filter condition should be the original one
        assertEquals(partialCondition, topFilter.condition());
        Join optimizedJoin = as(topFilter.child(), Join.class);
        assertEquals(left, optimizedJoin.left());
        Filter rightFilter = as(optimizedJoin.right(), Filter.class);
        // Only the pushable part should be a candidate
        assertEquals(pushableCondition, rightFilter.condition());
    }

    public void testPushDownFilterPastLeftJoinWithOr() {
        Join join = createLeftJoinOnFields();
        FieldAttribute c = (FieldAttribute) join.right().output().get(0);

        Expression pushableCondition = greaterThanOf(c, ONE);
        Expression nonPushableCondition = new IsNull(EMPTY, c);

        // OR of pushable and non-pushable filter
        Expression orCondition = new Or(EMPTY, pushableCondition, nonPushableCondition);
        Filter filter = new Filter(EMPTY, join, orCondition);
        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter, optimizerContext);
        // No optimization should be applied, the plan should be the same
        assertEquals(filter, optimized);
        // And the join inside should not have candidate filters
        Join innerJoin = as(filter.child(), Join.class);
        assertFalse(innerJoin.right() instanceof Filter);
    }

    public void testPushDownFilterPastLeftJoinWithNotButStillPushable() {
        Join join = createLeftJoinOnFields();
        FieldAttribute c = (FieldAttribute) join.right().output().get(0);

        Expression pushableCondition = greaterThanOf(c, ONE);

        // negation of pushable filter, in this case it remains pushable
        Expression negationOfPushableCondition = new Not(EMPTY, pushableCondition);
        Filter filter = new Filter(EMPTY, join, negationOfPushableCondition);
        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter, optimizerContext);
        Filter topFilter = as(optimized, Filter.class);
        assertEquals(negationOfPushableCondition, topFilter.condition());
        Join optimizedJoin = as(topFilter.child(), Join.class);
        Filter rightFilter = as(optimizedJoin.right(), Filter.class);
        assertEquals(negationOfPushableCondition, rightFilter.condition());
    }

    public void testPushDownFilterPastLeftJoinWithNotNonPushable() {
        Join join = createLeftJoinOnFields();
        FieldAttribute c = (FieldAttribute) join.right().output().get(0);

        Expression nonPushableCondition = new IsNull(EMPTY, c);

        // negation of non-pushable filter makes it pushable
        Expression negationOfNonPushableCondition = new Not(EMPTY, nonPushableCondition);
        Filter filter = new Filter(EMPTY, join, negationOfNonPushableCondition);
        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter, optimizerContext);
        Filter topFilter = as(optimized, Filter.class);
        assertEquals(negationOfNonPushableCondition, topFilter.condition());
        Join optimizedJoin = as(topFilter.child(), Join.class);
        Filter rightFilter = as(optimizedJoin.right(), Filter.class);
        assertEquals(negationOfNonPushableCondition, rightFilter.condition());
    }

    public void testPushDownFilterPastLeftJoinWithComplexMix() {
        // Setup
        FieldAttribute a = getFieldAttribute("a");
        FieldAttribute c = getFieldAttribute("c");
        FieldAttribute d = getFieldAttribute("d");
        FieldAttribute e = getFieldAttribute("e");
        FieldAttribute f = getFieldAttribute("f");
        FieldAttribute g = getFieldAttribute("g");
        EsRelation left = relation(List.of(a, getFieldAttribute("b")));
        EsRelation right = relation(List.of(c, d, e, f, g));
        JoinConfig joinConfig = new JoinConfig(JoinTypes.LEFT, List.of(a), List.of(c), null);
        Join join = new Join(EMPTY, left, right, joinConfig);

        // Predicates
        Expression p1 = greaterThanOf(c, ONE);                                  // pushable
        Expression p2 = new Not(EMPTY, new IsNull(EMPTY, d));     // pushable (d IS NOT NULL)
        Expression p3 = lessThanOf(e, THREE);                                   // pushable
        Expression p4 = rlike(f, "pat");                                   // pushable
        Expression p5 = new Not(EMPTY, new IsNull(EMPTY, g));     // pushable (g IS NOT NULL)
        Expression p6 = greaterThanOf(c, TWO);                                  // pushable
        Expression p7 = lessThanOf(d, FOUR);                                    // pushable
        Expression p8 = greaterThanOf(e, FIVE);                                 // pushable

        Expression np1 = new IsNull(EMPTY, c);                           // non-pushable (c IS NULL)
        Expression np2 = new Equals(EMPTY, new Coalesce(EMPTY, d, List.of(SIX)), SIX); // non-pushable

        // Build a complex condition
        // np2 AND ((p1 AND p2 AND p3 AND p4 AND p5) AND (np1 OR (p6 AND p7) OR (p8 AND np2))) AND p1 AND p6
        Expression pushableBranch = Predicates.combineAnd(List.of(p1, p2, p3, p4, p5));
        Expression nonPushableBranch = new Or(EMPTY, np1, new Or(EMPTY, new And(EMPTY, p6, p7), new And(EMPTY, p8, np2)));
        Expression complexCondition = new And(EMPTY, pushableBranch, nonPushableBranch);
        complexCondition = Predicates.combineAnd(List.of(np2, complexCondition, p1, p6));

        Filter filter = new Filter(EMPTY, join, complexCondition);
        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter, optimizerContext);

        // The top filter with the original condition should remain, but the structure of the AND tree might have changed.
        // So, we flatten the conditions and compare them as a set.
        Filter topFilter = as(optimized, Filter.class);
        Set<Expression> actualTopPredicates = new HashSet<>(Predicates.splitAnd(topFilter.condition()));
        Set<Expression> expectedTopPredicates = new HashSet<>(List.of(p1, p2, p3, p4, p5, nonPushableBranch, np2, p1, p6));
        assertEquals(expectedTopPredicates, actualTopPredicates);

        // The pushable part of the filter should be added as a candidate to the join
        Join optimizedJoin = as(topFilter.child(), Join.class);
        assertEquals(left, optimizedJoin.left());
        Filter rightFilter = as(optimizedJoin.right(), Filter.class);
        Set<Expression> actualPushable = new HashSet<>(Predicates.splitAnd(rightFilter.condition()));
        Set<Expression> expectedPushable = new HashSet<>(List.of(p1, p2, p3, p4, p5, p6));
        assertEquals(expectedPushable, actualPushable);
    }

    /**
     * Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16, languages
     * {f}#10 AS language_code#4, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_name{f}#19]]
     * \_Limit[1000[INTEGER],false]
     *   \_Filter[ISNULL(language_name{f}#19)]
     *     \_Join[LEFT,[languages{f}#10],[languages{f}#10],[language_code{f}#18]]
     *       |_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *       \_EsRelation[languages_lookup][LOOKUP][language_code{f}#18, language_name{f}#19]
     */
    public void testDoNotPushDownIsNullFilterPastLookupJoin() {
        var plan = plan("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name IS NULL
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var join = as(filter.child(), Join.class);
        assertThat(join.right(), instanceOf(EsRelation.class));
    }

    /**
     * Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16, languages
     * {f}#10 AS language_code#4, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_name{f}#19]]
     * \_Limit[1000[INTEGER],false]
     *   \_Filter[language_name{f}#19 > a[KEYWORD]]
     *     \_Join[LEFT,[languages{f}#10],[languages{f}#10],[language_code{f}#18],false]
     *       |_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *       \_Filter[language_name{f}#19 > a[KEYWORD]]
     *         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#18, language_name{f}#19]
     */
    public void testPushDownGreaterThanFilterPastLookupJoin() {
        var plan = plan("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name > "a"
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var join = as(filter.child(), Join.class);
        var rightFilter = as(join.right(), Filter.class);
        assertThat(rightFilter.condition().toString(), is("language_name > \"a\""));
    }

    /**
     * Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16, languages
     * {f}#10 AS language_code#4, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_name{f}#19]]
     * \_Limit[1000[INTEGER],false]
     *   \_Filter[COALESCE(language_name{f}#19,a[KEYWORD]) == a[KEYWORD]]
     *     \_Join[LEFT,[languages{f}#10],[languages{f}#10],[language_code{f}#18]]
     *       |_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *       \_EsRelation[languages_lookup][LOOKUP][language_code{f}#18, language_name{f}#19]
     */
    public void testDoNotPushDownCoalesceFilterPastLookupJoin() {
        var plan = plan("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE COALESCE(language_name, "a") == "a"
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var join = as(filter.child(), Join.class);
        assertThat(join.right(), instanceOf(EsRelation.class));
    }

    /**
     *
     * Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16, languages
     * {f}#10 AS language_code#4, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_name{f}#19]]
     * \_Limit[1000[INTEGER],false]
     *   \_Filter[ISNOTNULL(language_name{f}#19)]
     *     \_Join[LEFT,[languages{f}#10],[languages{f}#10],[language_code{f}#18],false]
     *       |_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *       \_Filter[ISNOTNULL(language_name{f}#19)]
     *         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#18, language_name{f}#19]
     */
    public void testPushDownIsNotNullFilterPastLookupJoin() {
        var plan = plan("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name IS NOT NULL
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var join = as(filter.child(), Join.class);
        var rightFilter = as(join.right(), Filter.class);
        assertThat(rightFilter.condition().toString(), is("language_name IS NOT NULL"));
    }

    /**
     * Project[[_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, gender{f}#13, hire_date{f}#18, job{f}#19, job.raw{f}#20, languages
     * {f}#14 AS language_code#4, last_name{f}#15, long_noidx{f}#21, salary{f}#16, language_name{f}#23]]
     * \_Limit[1000[INTEGER],false]
     *   \_Filter[ISNOTNULL(language_name{f}#23) AND language_name{f}#23 > a[KEYWORD] AND LIKE(language_name{f}#23, "*b", false)
     *  AND COALESCE(language_name{f}#23,c[KEYWORD]) == c[KEYWORD] AND RLIKE(language_name{f}#23, "f.*", false)]
     *     \_Join[LEFT,[languages{f}#14],[languages{f}#14],[language_code{f}#22]]
     *       |_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     *       \_Filter[ISNOTNULL(language_name{f}#23) AND language_name{f}#23 > a[KEYWORD] AND LIKE(language_name{f}#23, "*b", false)
     *  AND RLIKE(language_name{f}#23, "f.*", false)]
     *         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#22, language_name{f}#23]
     */
    public void testPushDownMultipleWhere() {
        var plan = plan("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name IS NOT NULL
            | WHERE language_name > "a"
            | WHERE language_name LIKE "*b"
            | WHERE COALESCE(language_name, "c") == "c"
            | WHERE language_name RLIKE "f.*"
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var topFilter = as(limit.child(), Filter.class);

        // Verify the top-level filter contains all 5 original conditions combined
        Set<String> expectedAllFilters = Set.of(
            "language_name IS NOT NULL",
            "language_name > \"a\"",
            "language_name LIKE \"*b\"",
            "COALESCE(language_name, \"c\") == \"c\"",
            "language_name RLIKE \"f.*\""
        );

        Set<String> actualAllFilters = new HashSet<>(Predicates.splitAnd(topFilter.condition()).stream().map(Object::toString).toList());
        assertEquals(expectedAllFilters, actualAllFilters);

        // Verify the join is below the top-level filter
        var join = as(topFilter.child(), Join.class);

        // Verify a new filter with only the pushable predicates has been pushed to the right side of the join
        var rightFilter = as(join.right(), Filter.class);
        Set<String> expectedPushedFilters = Set.of(
            "language_name IS NOT NULL",
            "language_name > \"a\"",
            "language_name LIKE \"*b\"",
            "language_name RLIKE \"f.*\""
        );
        Set<String> actualPushedFilters = new HashSet<>(
            Predicates.splitAnd(rightFilter.condition()).stream().map(Object::toString).toList()
        );
        assertEquals(expectedPushedFilters, actualPushedFilters);
    }

    /**
     *
     * Project[[$$languages$temp_name$32{r$}#33 AS language_code#4, salary{f}#13, language_name{f}#20, _meta_field{f}#27, emp
     * _no{f}#21, first_name{f}#22, gender{f}#23, hire_date{f}#28, job{f}#29, job.raw{f}#30, languages{f}#24,
     * last_name{f}#25, long_noidx{f}#31]]
     * \_Limit[1000[INTEGER],true]
     *   \_Join[LEFT,[salary{f}#13],[salary{f}#13],[salary{f}#26]]
     *     |_Eval[[languages{f}#11 AS $$languages$temp_name$32#33]]
     *     | \_Limit[1000[INTEGER],false]
     *     |   \_Filter[language_name{f}#20 > a[KEYWORD]]
     *     |     \_Join[LEFT,[languages{f}#11],[languages{f}#11],[language_code{f}#19]]
     *     |       |_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     *     |       \_Filter[language_name{f}#20 > a[KEYWORD]]
     *     |         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#19, language_name{f}#20]
     *     \_EsRelation[test_lookup][LOOKUP][_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, ..]
     */
    public void testPushDownFilterPastTwoLookupJoins() {
        var plan = plan("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | LOOKUP JOIN test_lookup ON salary
            | WHERE language_name > "a"
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);

        // The filter is pushed down past the top join, so a Join is now at the top of the plan after the limit
        var topJoin = as(limit.child(), Join.class);
        assertThat(topJoin.right(), instanceOf(EsRelation.class)); // No filter on the top lookup join's right side

        // Traverse down the left side of the top join to find the filter and the bottom join
        var eval = as(topJoin.left(), Eval.class);
        var innerLimit = as(eval.child(), Limit.class);
        var topFilter = as(innerLimit.child(), Filter.class);
        assertThat(topFilter.condition().toString(), is("language_name > \"a\""));

        // make sure that the filter was pushed to the right side of the bottom join
        var bottomJoin = as(topFilter.child(), Join.class);
        var rightFilter = as(bottomJoin.right(), Filter.class);
        assertThat(rightFilter.condition().toString(), is("language_name > \"a\""));
    }

    private Join createLeftJoinOnFields() {
        FieldAttribute a = getFieldAttribute("a");
        FieldAttribute b = getFieldAttribute("b");
        FieldAttribute c = getFieldAttribute("c");
        EsRelation left = relation(List.of(a, b));
        EsRelation right = relation(List.of(c, b));

        JoinConfig joinConfig = new JoinConfig(JoinTypes.LEFT, List.of(a, b), List.of(b, c), null);
        return new Join(EMPTY, left, right, joinConfig);
    }

    private Join createLeftJoinOnExpression() {
        FieldAttribute a = getFieldAttribute("a");
        FieldAttribute b1 = getFieldAttribute("b1");
        FieldAttribute b2 = getFieldAttribute("b2");
        FieldAttribute c = getFieldAttribute("c");
        EsRelation left = relation(List.of(a, b1));
        EsRelation right = relation(List.of(c, b2));
        Expression joinOnCondition = new GreaterThanOrEqual(Source.EMPTY, b1, b2);
        JoinConfig joinConfig = new JoinConfig(JoinTypes.LEFT, List.of(b1), List.of(b2), joinOnCondition);
        return new Join(EMPTY, left, right, joinConfig);
    }

    public void testLeftJoinOnExpressionPushable() {
        Join join = createLeftJoinOnExpression();
        EsRelation left = (EsRelation) join.left();
        FieldAttribute c = (FieldAttribute) join.right().output().get(0);

        // Pushable filter
        Expression pushableCondition = greaterThanOf(c, ONE);
        Filter filter = new Filter(EMPTY, join, pushableCondition);
        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter, optimizerContext);
        // The filter should still be on top
        Filter topFilter = as(optimized, Filter.class);
        assertEquals(pushableCondition, topFilter.condition());
        Join optimizedJoin = as(topFilter.child(), Join.class);
        assertEquals(left, optimizedJoin.left());
        Filter rightFilter = as(optimizedJoin.right(), Filter.class);
        assertEquals(pushableCondition, rightFilter.condition());
    }

    public void testLeftJoinOnExpressionPushableLeftAndRight() {
        Join join = createLeftJoinOnExpression();
        FieldAttribute a = (FieldAttribute) join.left().output().get(0);
        FieldAttribute c = (FieldAttribute) join.right().output().get(0);

        // Pushable filters on both left and right
        Expression leftPushableCondition = greaterThanOf(a, ONE);
        Expression rightPushableCondition = greaterThanOf(c, TWO);
        Expression combinedCondition = Predicates.combineAnd(List.of(leftPushableCondition, rightPushableCondition));
        Filter filter = new Filter(EMPTY, join, combinedCondition);
        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter, optimizerContext);

        // The top filter should only contain the right pushable condition
        // because left pushable conditions are completely pushed down and removed from the top
        Filter topFilter = as(optimized, Filter.class);
        assertEquals(rightPushableCondition, topFilter.condition());
        Join optimizedJoin = as(topFilter.child(), Join.class);

        // Check that the left side has the left pushable filter
        Filter leftFilter = as(optimizedJoin.left(), Filter.class);
        assertEquals(leftPushableCondition, leftFilter.condition());

        // Check that the right side has the right pushable filter
        Filter rightFilter = as(optimizedJoin.right(), Filter.class);
        assertEquals(rightPushableCondition, rightFilter.condition());
    }

    public void testPushDownFilterPastLeftJoinExpressionWithPartiallyPushableAnd() {
        Join join = createLeftJoinOnExpression();
        EsRelation left = (EsRelation) join.left();
        FieldAttribute c = (FieldAttribute) join.right().output().get(0);

        Expression pushableCondition = greaterThanOf(c, ONE);
        Expression nonPushableCondition = new IsNull(EMPTY, c);

        // Partially pushable filter
        Expression partialCondition = new And(EMPTY, pushableCondition, nonPushableCondition);
        Filter filter = new Filter(EMPTY, join, partialCondition);
        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter, optimizerContext);
        Filter topFilter = as(optimized, Filter.class);
        // The top filter condition should be the original one
        assertEquals(partialCondition, topFilter.condition());
        Join optimizedJoin = as(topFilter.child(), Join.class);
        assertEquals(left, optimizedJoin.left());
        Filter rightFilter = as(optimizedJoin.right(), Filter.class);
        // Only the pushable part should be a candidate
        assertEquals(pushableCondition, rightFilter.condition());
    }

    /**
     *Limit[1000[INTEGER],false]
     * \_Filter[ISNULL(language_name{f}#17)]
     *   \_Join[LEFT,[languages{f}#8, language_code{f}#16],[languages{f}#8],[language_code{f}#16],languages{f}#8 > language_code{f
     * }#16]
     *     |_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#16, language_name{f}#17]
     *
     */
    public void testDoNotPushDownIsNullFilterPastLookupJoinExpression() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        var plan = plan("""
            FROM test
            | LOOKUP JOIN languages_lookup ON languages > language_code
            | WHERE language_name IS NULL
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var join = as(filter.child(), Join.class);
        assertThat(join.right(), instanceOf(EsRelation.class));
    }

    /**
     * Limit[1000[INTEGER],false]
     * \_Filter[ISNOTNULL(language_name{f}#21) AND language_name{f}#21 > a[KEYWORD] AND LIKE(language_name{f}#21, "*b", false)
     *  AND COALESCE(language_name{f}#21,c[KEYWORD]) == c[KEYWORD] AND RLIKE(language_name{f}#21, "f.*", false)]
     *   \_Join[LEFT,[languages{f}#12, language_code{f}#20],[languages{f}#12],[language_code{f}#20],languages{f}#12 == language_code
     * {f}#20]
     *     |_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *     \_Filter[ISNOTNULL(language_name{f}#21) AND language_name{f}#21 > a[KEYWORD] AND LIKE(language_name{f}#21, "*b", false)
     *  AND RLIKE(language_name{f}#21, "f.*", false)]
     *       \_EsRelation[languages_lookup][LOOKUP][language_code{f}#20, language_name{f}#21]
     */
    public void testPushDownLookupJoinExpressionMultipleWhere() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        var plan = plan("""
            FROM test
            | LOOKUP JOIN languages_lookup ON languages <= language_code
            | WHERE language_name IS NOT NULL
            | WHERE language_name > "a"
            | WHERE language_name LIKE "*b"
            | WHERE COALESCE(language_name, "c") == "c"
            | WHERE language_name RLIKE "f.*"
            """);

        var limit = as(plan, Limit.class);
        var topFilter = as(limit.child(), Filter.class);

        // Verify the top-level filter contains all 5 original conditions combined
        Set<String> expectedAllFilters = Set.of(
            "language_name IS NOT NULL",
            "language_name > \"a\"",
            "language_name LIKE \"*b\"",
            "COALESCE(language_name, \"c\") == \"c\"",
            "language_name RLIKE \"f.*\""
        );

        Set<String> actualAllFilters = new HashSet<>(Predicates.splitAnd(topFilter.condition()).stream().map(Object::toString).toList());
        assertEquals(expectedAllFilters, actualAllFilters);

        // Verify the join is below the top-level filter
        var join = as(topFilter.child(), Join.class);

        // Verify a new filter with only the pushable predicates has been pushed to the right side of the join
        var rightFilter = as(join.right(), Filter.class);
        Set<String> expectedPushedFilters = Set.of(
            "language_name IS NOT NULL",
            "language_name > \"a\"",
            "language_name LIKE \"*b\"",
            "language_name RLIKE \"f.*\""
        );
        Set<String> actualPushedFilters = new HashSet<>(
            Predicates.splitAnd(rightFilter.condition()).stream().map(Object::toString).toList()
        );
        assertEquals(expectedPushedFilters, actualPushedFilters);
    }

    /*
     * We check that a filter after an INLINE JOIN is pushed down past the join into the sub-plan.
     * With the help of InlineJoin.firstSubPlan(), we extract the sub-plan and verify that the filter exists there, meaning EsqlSession
     * did its job of pushing down the filter (by copying the correct part of the left hand side to the right hand side to also include
     * the aforementioned filter).
     *
     * EsqlProject[[avg{r}#5, languages{f}#15, salary{f}#17, emp_no{f}#12]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_InlineJoin[LEFT,[languages{f}#15],[languages{r}#15]]
     *     |_Filter[languages{f}#15 > 2[INTEGER]]
     *     | \_EsRelation[employees][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     *     \_Project[[avg{r}#5, languages{f}#15]]
     *       \_Eval[[$$SUM$avg$0{r$}#23 / $$COUNT$avg$1{r$}#24 AS avg#5]]
     *         \_Aggregate[[languages{f}#15],[SUM(salary{f}#17,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avg$0#23,
     * COUNT(salary{f}#17,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#24, languages{f}#15]]
     *           \_StubRelation[[_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, gender{f}#14, hire_date{f}#19, job{f}#20, job.raw{f}#21,
     * languages{f}#15, last_name{f}#16, long_noidx{f}#22, salary{f}#17]]
     *
     * stubReplacedSubPlan:
     * Project[[avg{r}#5, languages{f}#15]]
     * \_Eval[[$$SUM$avg$0{r$}#23 / $$COUNT$avg$1{r$}#24 AS avg#5]]
     *   \_Aggregate[[languages{f}#15],[SUM(salary{f}#17,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avg$0#23,
     * COUNT(salary{f}#17,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#24, languages{f}#15]]
     *     \_Filter[languages{f}#15 > 2[INTEGER]]
     *       \_EsRelation[employees][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..], originalSubPlan=Project[[avg{r}#5,
     * languages{f}#15]]
     */
    public void testPushDown_OneGroupingFilter_PastInlineJoin() {
        var plan = plan("""
            FROM employees
            | INLINE STATS avg = AVG(salary) BY languages
            | WHERE languages > 2
            | KEEP avg, languages, salary, emp_no
            """);

        var subPlansResults = new HashSet<LocalRelation>();
        var firstSubPlan = InlineJoin.firstSubPlan(plan, subPlansResults).stubReplacedSubPlan();

        var project = as(plan, EsqlProject.class);
        var limit = as(project.child(), Limit.class);

        // InlineJoin left side
        var ij = as(limit.child(), InlineJoin.class);
        var left = as(ij.left(), Filter.class);
        assertThat(left.condition(), instanceOf(GreaterThan.class));
        var gt = as(left.condition(), GreaterThan.class);
        var fieldAttr = as(gt.left(), FieldAttribute.class);
        assertEquals("languages", fieldAttr.name());
        var gtRight = as(gt.right(), Literal.class);
        assertEquals(2, gtRight.value());
        as(left.child(), EsRelation.class);

        // InlineJoin right side
        var right = as(ij.right(), Project.class);
        var firstSubPlanProject = as(firstSubPlan, Project.class);
        assertEquals(right.output(), firstSubPlanProject.output());
        var eval = as(right.child(), Eval.class);

        // What EsqlSession is doing
        var firstSubPlanEval = as(firstSubPlanProject.child(), Eval.class);
        assertEquals(eval.fields(), firstSubPlanEval.fields());
        var aggregate = as(eval.child(), Aggregate.class);

        var firstSubPlanAggregate = as(firstSubPlanEval.child(), Aggregate.class);
        assertEquals(aggregate.groupings(), firstSubPlanAggregate.groupings());
        assertEquals(aggregate.aggregates(), firstSubPlanAggregate.aggregates());
        var firstSubPlanFilter = as(firstSubPlanAggregate.child(), Filter.class);
        // important bit below: the filter that is executed in the right hand side is the same as the one in the left hand side
        assertEquals(left, firstSubPlanFilter);
    }

    /*
     * EsqlProject[[avg{r}#5, languages{f}#18, gender{f}#17, emp_no{f}#15]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[emp_no{f}#15 > 10050[INTEGER]]
     *     \_InlineJoin[LEFT,[languages{f}#18, gender{f}#17],[languages{r}#18, gender{r}#17]]
     *       |_Filter[languages{f}#18 > 2[INTEGER] AND ISNOTNULL(gender{f}#17)]
     *       | \_EsRelation[employees][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     *       \_Project[[avg{r}#5, languages{f}#18, gender{f}#17]]
     *         \_Eval[[$$SUM$avg$0{r$}#26 / $$COUNT$avg$1{r$}#27 AS avg#5]]
     *           \_Aggregate[[languages{f}#18, gender{f}#17],[SUM(salary{f}#20,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$
     * SUM$avg$0#26, COUNT(salary{f}#20,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#27, languages{f}#18, gender{f}#17]]
     *             \_StubRelation[[_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, gender{f}#17, hire_date{f}#22, job{f}#23,
     * job.raw{f}#24, languages{f}#18, last_name{f}#19, long_noidx{f}#25, salary{f}#20]]
     *
     * stubReplacedSubPlan:
     * Project[[avg{r}#5, languages{f}#18, gender{f}#17]]
     * \_Eval[[$$SUM$avg$0{r$}#26 / $$COUNT$avg$1{r$}#27 AS avg#5]]
     *   \_Aggregate[[languages{f}#18, gender{f}#17],[SUM(salary{f}#20,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$
     * SUM$avg$0#26, COUNT(salary{f}#20,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#27, languages{f}#18, gender{f}#17]]
     *     \_Filter[languages{f}#18 > 2[INTEGER] AND ISNOTNULL(gender{f}#17)]
     *       \_EsRelation[employees][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     */
    public void testPushDown_SelectiveGroupingAndFilters_PastInlineJoin() {
        var plan = plan("""
            FROM employees
            | INLINE STATS avg = AVG(salary) BY languages, gender
            | WHERE languages > 2 AND gender IS NOT NULL AND emp_no > 10050
            | KEEP avg, languages, gender, emp_no
            """);

        var subPlansResults = new HashSet<LocalRelation>();
        var firstSubPlan = InlineJoin.firstSubPlan(plan, subPlansResults).stubReplacedSubPlan();

        var project = as(plan, EsqlProject.class);
        var limit = as(project.child(), Limit.class);

        // common filter, above InlineJoin
        var commonFilter = as(limit.child(), Filter.class);
        assertThat(commonFilter.condition(), instanceOf(GreaterThan.class));
        var commonGt = as(commonFilter.condition(), GreaterThan.class);
        assertEquals("emp_no", ((FieldAttribute) commonGt.left()).name());
        var commonGtRight = as(commonGt.right(), Literal.class);
        assertEquals(10050, commonGtRight.value());

        // InlineJoin left side
        var ij = as(commonFilter.child(), InlineJoin.class);
        var left = as(ij.left(), Filter.class);
        assertThat(left.condition(), instanceOf(And.class));
        var and = as(left.condition(), And.class);
        var andLeft = as(and.left(), GreaterThan.class);
        var andLeftField = as(andLeft.left(), FieldAttribute.class);
        assertEquals("languages", andLeftField.name());
        var andLeftRight = as(andLeft.right(), Literal.class);
        assertEquals(2, andLeftRight.value());
        var andRight = as(and.right(), IsNotNull.class);
        var isNotNullField = as(andRight.field(), FieldAttribute.class);
        assertEquals("gender", isNotNullField.name());
        as(left.child(), EsRelation.class);

        // InlineJoin right side
        var right = as(ij.right(), Project.class);

        // What EsqlSession is doing
        var firstSubPlanProject = as(firstSubPlan, Project.class);
        assertEquals(right.output(), firstSubPlanProject.output());
        var eval = as(right.child(), Eval.class);

        var firstSubPlanEval = as(firstSubPlanProject.child(), Eval.class);
        assertEquals(eval.fields(), firstSubPlanEval.fields());
        var aggregate = as(eval.child(), Aggregate.class);

        var firstSubPlanAggregate = as(firstSubPlanEval.child(), Aggregate.class);
        assertEquals(aggregate.groupings(), firstSubPlanAggregate.groupings());
        assertEquals(aggregate.aggregates(), firstSubPlanAggregate.aggregates());
        var firstSubPlanFilter = as(firstSubPlanAggregate.child(), Filter.class);
        // important bit below: the filter that is executed in the right hand side is the same as the one in the left hand side
        assertEquals(left, firstSubPlanFilter);
    }

    /*
     * EsqlProject[[avg{r}#5, languages{f}#18, gender{f}#17, emp_no{f}#15]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[ISNOTNULL(gender{f}#17) OR emp_no{f}#15 > 10050[INTEGER]]
     *     \_InlineJoin[LEFT,[languages{f}#18, gender{f}#17],[languages{r}#18, gender{r}#17]]
     *       |_Filter[languages{f}#18 > 2[INTEGER]]
     *       | \_EsRelation[employees][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     *       \_Project[[avg{r}#5, languages{f}#18, gender{f}#17]]
     *         \_Eval[[$$SUM$avg$0{r$}#26 / $$COUNT$avg$1{r$}#27 AS avg#5]]
     *           \_Aggregate[[languages{f}#18, gender{f}#17],[SUM(salary{f}#20,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$
     * SUM$avg$0#26, COUNT(salary{f}#20,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#27, languages{f}#18, gender{f}#17]]
     *             \_StubRelation[[_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, gender{f}#17, hire_date{f}#22, job{f}#23,
     * job.raw{f}#24, languages{f}#18, last_name{f}#19, long_noidx{f}#25, salary{f}#20]]
     *
     * stubReplacedSubPlan:
     * Project[[avg{r}#5, languages{f}#18, gender{f}#17]]
     * \_Eval[[$$SUM$avg$0{r$}#26 / $$COUNT$avg$1{r$}#27 AS avg#5]]
     *   \_Aggregate[[languages{f}#18, gender{f}#17],[SUM(salary{f}#20,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$
     * SUM$avg$0#26, COUNT(salary{f}#20,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#27, languages{f}#18, gender{f}#17]]
     *     \_Filter[languages{f}#18 > 2[INTEGER]]
     *       \_EsRelation[employees][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     */
    public void testPushDown_SelectiveGroupingOrFilters_PastInlineJoin() {
        var plan = plan("""
            FROM employees
            | INLINE STATS avg = AVG(salary) BY languages, gender
            | WHERE languages > 2 AND (gender IS NOT NULL OR emp_no > 10050)
            | KEEP avg, languages, gender, emp_no
            """);

        var subPlansResults = new HashSet<LocalRelation>();
        var firstSubPlan = InlineJoin.firstSubPlan(plan, subPlansResults).stubReplacedSubPlan();

        var project = as(plan, EsqlProject.class);
        var limit = as(project.child(), Limit.class);

        // common filter, above InlineJoin
        var commonFilter = as(limit.child(), Filter.class);
        assertThat(commonFilter.condition(), instanceOf(Or.class));
        var commonOr = as(commonFilter.condition(), Or.class);
        assertThat(commonOr.left(), instanceOf(IsNotNull.class));
        var commonIsNotNull = as(commonOr.left(), IsNotNull.class);
        var commonIsNotNullField = as(commonIsNotNull.field(), FieldAttribute.class);
        assertEquals("gender", commonIsNotNullField.name());
        assertThat(commonOr.right(), instanceOf(GreaterThan.class));
        var commonGt = as(commonOr.right(), GreaterThan.class);
        var commonGtLeft = as(commonGt.left(), FieldAttribute.class);
        assertEquals("emp_no", commonGtLeft.name());
        var commonGtRight = as(commonGt.right(), Literal.class);
        assertEquals(10050, commonGtRight.value());

        // InlineJoin left side
        var ij = as(commonFilter.child(), InlineJoin.class);
        var left = as(ij.left(), Filter.class);
        assertThat(left.condition(), instanceOf(GreaterThan.class));
        var gt = as(left.condition(), GreaterThan.class);
        var fieldAttr = as(gt.left(), FieldAttribute.class);
        assertEquals("languages", fieldAttr.name());
        var gtRight = as(gt.right(), Literal.class);
        assertEquals(2, gtRight.value());
        as(left.child(), EsRelation.class);

        // InlineJoin right side
        var right = as(ij.right(), Project.class);

        // What EsqlSession is doing
        var firstSubPlanProject = as(firstSubPlan, Project.class);
        assertEquals(right.output(), firstSubPlanProject.output());
        var eval = as(right.child(), Eval.class);

        var firstSubPlanEval = as(firstSubPlanProject.child(), Eval.class);
        assertEquals(eval.fields(), firstSubPlanEval.fields());
        var aggregate = as(eval.child(), Aggregate.class);

        var firstSubPlanAggregate = as(firstSubPlanEval.child(), Aggregate.class);
        assertEquals(aggregate.groupings(), firstSubPlanAggregate.groupings());
        assertEquals(aggregate.aggregates(), firstSubPlanAggregate.aggregates());
        var firstSubPlanFilter = as(firstSubPlanAggregate.child(), Filter.class);
        // important bit below: the filter that is executed in the right hand side is the same as the one in the left hand side
        assertEquals(left, firstSubPlanFilter);
    }

    /*
     * EsqlProject[[avg{r}#7, languages{f}#21, gender{f}#20, emp_no{f}#18]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[ISNOTNULL(gender{f}#20) OR emp_no{f}#18 > 10050[INTEGER] AND salary{f}#23 > 5000[INTEGER]]
     *     \_InlineJoin[LEFT,[languages{f}#21, gender{f}#20],[languages{r}#21, gender{r}#20]]
     *       |_Filter[emp_no{f}#18 < 10090[INTEGER] AND salary{f}#23 < 10000[INTEGER] AND languages{f}#21 > 2[INTEGER]]
     *       | \_EsRelation[employees][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     *       \_Project[[avg{r}#7, languages{f}#21, gender{f}#20]]
     *         \_Eval[[$$SUM$avg$0{r$}#29 / $$COUNT$avg$1{r$}#30 AS avg#7]]
     *           \_Aggregate[[languages{f}#21, gender{f}#20],[SUM(salary{f}#23,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$
     * SUM$avg$0#29, COUNT(salary{f}#23,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#30, languages{f}#21, gender{f}#20]]
     *             \_StubRelation[[_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, gender{f}#20, hire_date{f}#25, job{f}#26,
     * job.raw{f}#27, languages{f}#21, last_name{f}#22, long_noidx{f}#28, salary{f}#23]]
     *
     * stubReplacedSubPlan:
     * Project[[avg{r}#7, languages{f}#21, gender{f}#20]]
     * \_Eval[[$$SUM$avg$0{r$}#29 / $$COUNT$avg$1{r$}#30 AS avg#7]]
     *   \_Aggregate[[languages{f}#21, gender{f}#20],[SUM(salary{f}#23,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$
     * SUM$avg$0#29, COUNT(salary{f}#23,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#30, languages{f}#21, gender{f}#20]]
     *     \_Filter[emp_no{f}#18 < 10090[INTEGER] AND salary{f}#23 < 10000[INTEGER] AND languages{f}#21 > 2[INTEGER]]
     *       \_EsRelation[employees][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     */
    public void testPushDown_ComplexFiltering_CombinedWithLeftFiltering_PastInlineJoin() {
        var plan = plan("""
            FROM employees
            | WHERE emp_no < 10090 AND salary < 10000
            | INLINE STATS avg = AVG(salary) BY languages, gender
            | WHERE languages > 2 AND (gender IS NOT NULL OR emp_no > 10050) AND salary > 5000
            | KEEP avg, languages, gender, emp_no
            """);

        var subPlansResults = new HashSet<LocalRelation>();
        var firstSubPlan = InlineJoin.firstSubPlan(plan, subPlansResults).stubReplacedSubPlan();

        var project = as(plan, EsqlProject.class);
        var limit = as(project.child(), Limit.class);

        // common filter, above InlineJoin
        // ISNOTNULL(gender{f}#20) OR emp_no{f}#18 > 10050[INTEGER] AND salary{f}#23 > 5000[INTEGER]
        var commonFilter = as(limit.child(), Filter.class);
        assertThat(commonFilter.condition(), instanceOf(And.class));
        var commonAnd = as(commonFilter.condition(), And.class);
        assertThat(commonAnd.left(), instanceOf(Or.class));
        var commonOr = as(commonAnd.left(), Or.class);
        assertThat(commonOr.left(), instanceOf(IsNotNull.class));
        var commonIsNotNull = as(commonOr.left(), IsNotNull.class);
        var commonIsNotNullField = as(commonIsNotNull.field(), FieldAttribute.class);
        assertEquals("gender", commonIsNotNullField.name());
        assertThat(commonOr.right(), instanceOf(GreaterThan.class));
        var commonGt = as(commonOr.right(), GreaterThan.class);
        var commonGtLeft = as(commonGt.left(), FieldAttribute.class);
        assertEquals("emp_no", commonGtLeft.name());
        var commonGtRight = as(commonGt.right(), Literal.class);
        assertEquals(10050, commonGtRight.value());
        assertThat(commonAnd.right(), instanceOf(GreaterThan.class));
        var commonAndGt = as(commonAnd.right(), GreaterThan.class);
        var commonAndGtLeft = as(commonAndGt.left(), FieldAttribute.class);
        assertEquals("salary", commonAndGtLeft.name());
        var commonAndGtRight = as(commonAndGt.right(), Literal.class);
        assertEquals(5000, commonAndGtRight.value());

        // InlineJoin left side
        var ij = as(commonFilter.child(), InlineJoin.class);
        var left = as(ij.left(), Filter.class);

        // Filter[emp_no{f}#18 < 10090[INTEGER] AND salary{f}#23 < 10000[INTEGER] AND languages{f}#21 > 2[INTEGER]]
        assertThat(left.condition(), instanceOf(And.class));
        var and = as(left.condition(), And.class);
        assertThat(and.left(), instanceOf(And.class));
        var andLeft = as(and.left(), And.class);
        var andLeftLTLeft = as(andLeft.left(), LessThan.class);
        var andLeftLTField = as(andLeftLTLeft.left(), FieldAttribute.class);
        assertEquals("emp_no", andLeftLTField.name());
        var andLeftLTLeftRight = as(andLeftLTLeft.right(), Literal.class);
        assertEquals(10090, andLeftLTLeftRight.value());
        var andLeftLTRight = as(andLeft.right(), LessThan.class);
        andLeftLTField = as(andLeftLTRight.left(), FieldAttribute.class);
        assertEquals("salary", andLeftLTField.name());
        var andLeftLTRightRight = as(andLeftLTRight.right(), Literal.class);
        assertEquals(10000, andLeftLTRightRight.value());
        assertThat(and.right(), instanceOf(GreaterThan.class));
        var andRight = as(and.right(), GreaterThan.class);
        var andRightField = as(andRight.left(), FieldAttribute.class);
        assertEquals("languages", andRightField.name());
        var andRightRight = as(andRight.right(), Literal.class);
        assertEquals(2, andRightRight.value());
        as(left.child(), EsRelation.class);

        // InlineJoin right side
        var right = as(ij.right(), Project.class);

        // What EsqlSession is doing
        var firstSubPlanProject = as(firstSubPlan, Project.class);
        assertEquals(right.output(), firstSubPlanProject.output());
        var eval = as(right.child(), Eval.class);

        var firstSubPlanEval = as(firstSubPlanProject.child(), Eval.class);
        assertEquals(eval.fields(), firstSubPlanEval.fields());
        var aggregate = as(eval.child(), Aggregate.class);

        var firstSubPlanAggregate = as(firstSubPlanEval.child(), Aggregate.class);
        assertEquals(aggregate.groupings(), firstSubPlanAggregate.groupings());
        assertEquals(aggregate.aggregates(), firstSubPlanAggregate.aggregates());
        var firstSubPlanFilter = as(firstSubPlanAggregate.child(), Filter.class);
        // important bit below: the filter that is executed in the right hand side is the same as the one in the left hand side
        assertEquals(left, firstSubPlanFilter);
    }

    /*
     * EsqlProject[[salary{f}#16, emp_no{f}#11]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_InlineJoin[LEFT,[salary{f}#16],[salary{r}#16]]
     *     |_Filter[salary{f}#16 < 10000[INTEGER] AND salary{f}#16 > 10000[INTEGER]]
     *     | \_EsRelation[employees][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     *     \_Aggregate[[salary{f}#16],[salary{f}#16]]
     *       \_StubRelation[[_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, gender{f}#13, hire_date{f}#18, job{f}#19, job.raw{f}#20, l
     * anguages{f}#14, last_name{f}#15, long_noidx{f}#21, salary{f}#16]]
     *
     * stubReplacedSubPlan:
     * Aggregate[[salary{f}#16],[salary{f}#16]]
     * \_Filter[salary{f}#16 < 10000[INTEGER] AND salary{f}#16 > 10000[INTEGER]]
     *   \_EsRelation[employees][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     */
    public void testPushDown_ImpossibleFilter_PastInlineJoin() {
        var plan = plan("""
            FROM employees
            | WHERE salary < 10000
            | INLINE STATS salary = AVG(salary) BY salary
            | WHERE salary > 10000
            | KEEP salary, emp_no
            """);
        var subPlansResults = new HashSet<LocalRelation>();
        var firstSubPlan = InlineJoin.firstSubPlan(plan, subPlansResults).stubReplacedSubPlan();

        var project = as(plan, EsqlProject.class);
        var limit = as(project.child(), Limit.class);
        // InlineJoin
        var ij = as(limit.child(), InlineJoin.class);

        // InlineJoin left side
        var left = as(ij.left(), Filter.class);
        // Filter[salary{f}#16 < 10000[INTEGER] AND salary{f}#16 > 10000[INTEGER]]
        assertThat(left.condition(), instanceOf(And.class));
        var and = as(left.condition(), And.class);
        var andLeft = as(and.left(), LessThan.class);
        var andLeftField = as(andLeft.left(), FieldAttribute.class);
        assertEquals("salary", andLeftField.name());
        var andLeftRight = as(andLeft.right(), Literal.class);
        assertEquals(10000, andLeftRight.value());
        var andRight = as(and.right(), GreaterThan.class);
        var andRightField = as(andRight.left(), FieldAttribute.class);
        assertEquals("salary", andRightField.name());
        var andRightRight = as(andRight.right(), Literal.class);
        assertEquals(10000, andRightRight.value());
        as(left.child(), EsRelation.class);

        // InlineJoin right side
        var right = as(ij.right(), Aggregate.class);

        // What EsqlSession is doing
        var firstSubPlanAggregate = as(firstSubPlan, Aggregate.class);
        assertEquals(right.output(), firstSubPlanAggregate.output());
        var firstSubPlanFilter = as(firstSubPlanAggregate.child(), Filter.class);
        // important bit below: the filter that is executed in the right hand side is the same as the one in the left hand side
        assertEquals(left, firstSubPlanFilter);

        assertWarnings("Line 3:16: Field 'salary' shadowed by field at line 3:40", "No limit defined, adding default limit of [1000]");
    }

    /*
     * EsqlProject[[languages{f}#14, a{r}#5, gender{f}#13]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[languages{f}#14 > 2[INTEGER]]
     *     \_InlineJoin[LEFT,[gender{f}#13],[gender{r}#13]]
     *       |_EsRelation[employees][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     *       \_Project[[a{r}#5, gender{f}#13]]
     *         \_Eval[[$$SUM$a$0{r$}#22 / $$COUNT$a$1{r$}#23 AS a#5]]
     *           \_Aggregate[[gender{f}#13],[SUM(salary{f}#16,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$a$0#22, COUNT
     * (salary{f}#16,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$a$1#23, gender{f}#13]]
     *             \_StubRelation[[_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, gender{f}#13, hire_date{f}#18, job{f}#19,
     * job.raw{f}#20, languages{f}#14, last_name{f}#15, long_noidx{f}#21, salary{f}#16]]
     *
     * stubReplacedSubPlan:
     * Project[[a{r}#5, gender{f}#13]]
     * \_Eval[[$$SUM$a$0{r$}#22 / $$COUNT$a$1{r$}#23 AS a#5]]
     *   \_Aggregate[[gender{f}#13],[SUM(salary{f}#16,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$a$0#22, COUNT
     * (salary{f}#16,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$a$1#23, gender{f}#13]]
     *     \_EsRelation[employees][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     */
    public void testDontPushDown_A_SimpleFilter_PastInlineJoin() {
        var plan = plan("""
            FROM employees
            | INLINE STATS a = AVG(salary) BY gender
            | WHERE languages > 2
            | KEEP languages, a, gender
            """);

        var subPlansResults = new HashSet<LocalRelation>();
        var firstSubPlan = InlineJoin.firstSubPlan(plan, subPlansResults).stubReplacedSubPlan();

        var project = as(plan, EsqlProject.class);
        var limit = as(project.child(), Limit.class);
        // common filter, above InlineJoin
        var commonFilter = as(limit.child(), Filter.class);
        assertThat(commonFilter.condition(), instanceOf(GreaterThan.class));
        var commonGt = as(commonFilter.condition(), GreaterThan.class);
        var commonGtField = as(commonGt.left(), FieldAttribute.class);
        assertEquals("languages", commonGtField.name());
        var commonGtRight = as(commonGt.right(), Literal.class);
        assertEquals(2, commonGtRight.value());

        // InlineJoin
        var ij = as(commonFilter.child(), InlineJoin.class);
        // InlineJoin left side
        var left = as(ij.left(), EsRelation.class);
        // InlineJoin right side
        var right = as(ij.right(), Project.class);
        // What EsqlSession is doing
        var firstSubPlanProject = as(firstSubPlan, Project.class);
        assertEquals(right.output(), firstSubPlanProject.output());
        var eval = as(right.child(), Eval.class);
        var firstSubPlanEval = as(firstSubPlanProject.child(), Eval.class);
        assertEquals(eval.fields(), firstSubPlanEval.fields());
        var aggregate = as(eval.child(), Aggregate.class);
        var firstSubPlanAggregate = as(firstSubPlanEval.child(), Aggregate.class);
        assertEquals(aggregate.groupings(), firstSubPlanAggregate.groupings());
        assertEquals(aggregate.aggregates(), firstSubPlanAggregate.aggregates());
        var firstSubPlanRelation = as(firstSubPlanAggregate.child(), EsRelation.class);
        // important bit below: no filter has been pushed down, so the EsRelation is directly below the Aggregate
        assertEquals(left, firstSubPlanRelation);
    }

    /*
     * EsqlProject[[avgByL{r}#5, avgByG{r}#9, languages{f}#20, gender{f}#19, emp_no{f}#17]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[languages{f}#20 > 2[INTEGER]]
     *     \_InlineJoin[LEFT,[gender{f}#19],[gender{r}#19]]
     *       |_Filter[ISNOTNULL(gender{f}#19)]
     *       | \_InlineJoin[LEFT,[languages{f}#20],[languages{r}#20]]
     *       |   |_EsRelation[employees][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *       |   \_Project[[avgByL{r}#5, languages{f}#20]]
     *       |     \_Eval[[$$SUM$avgByL$0{r$}#29 / $$COUNT$avgByL$1{r$}#30 AS avgByL#5]]
     *       |       \_Aggregate[[languages{f}#20],[SUM(salary{f}#22,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS
     * $$SUM$avgByL$0#29, COUNT(salary{f}#22,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByL$1#30, languages{f}#20]]
     *       |         \_StubRelation[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25,
     * job.raw{f}#26, languages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22]]
     *       \_Project[[avgByG{r}#9, gender{f}#19]]
     *         \_Eval[[$$SUM$avgByG$0{r$}#31 / $$COUNT$avgByG$1{r$}#32 AS avgByG#9]]
     *           \_Aggregate[[gender{f}#19],[SUM(salary{f}#22,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avgByG$0#31,
     * COUNT(salary{f}#22,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByG$1#32, gender{f}#19]]
     *             \_StubRelation[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25,
     * job.raw{f}#26, last_name{f}#21, long_noidx{f}#27, salary{f}#22, avgByL{r}#5, languages{f}#20]]
     *
     * First InlineJoin stubReplacedSubPlan:
     * Project[[avgByL{r}#5, languages{f}#20]]
     * \_Eval[[$$SUM$avgByL$0{r$}#29 / $$COUNT$avgByL$1{r$}#30 AS avgByL#5]]
     *   \_Aggregate[[languages{f}#20],[SUM(salary{f}#22,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avgByL$0#2
     * 9, COUNT(salary{f}#22,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByL$1#30, languages{f}#20]]
     *     \_EsRelation[employees][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *
     * Second InlineJoin stubReplacedSubPlan:
     * Project[[avgByG{r}#9, gender{f}#19]]
     * \_Eval[[$$SUM$avgByG$0{r$}#31 / $$COUNT$avgByG$1{r$}#32 AS avgByG#9]]
     *   \_Aggregate[[gender{f}#19],[SUM(salary{f}#22,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avgByG$0#31,
     * COUNT(salary{f}#22,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByG$1#32, gender{f}#19]]
     *     \_Filter[ISNOTNULL(gender{f}#19)]
     *       \_InlineJoin[LEFT,[languages{f}#20],[languages{r}#20]]
     *         |_EsRelation[employees][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *         \_LocalRelation[[avgByL{r}#5, languages{f}#20],org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier@4330b7e0]
     */
    public void testPartiallyPushDown_GroupingFilters_PastTwoInlineJoins1() {
        // the optimized query should roughly look like this:
        /*
        FROM employees
            | INLINE STATS avgByL = AVG(salary) BY languages
            | WHERE gender IS NOT NULL
            | INLINE STATS avgByG = AVG(salary) BY gender
            | WHERE languages > 2
            | KEEP avg*, languages, gender, emp_no
         */
        var plan = plan("""
            FROM employees
            | INLINE STATS avgByL = AVG(salary) BY languages
            | INLINE STATS avgByG = AVG(salary) BY gender
            | WHERE languages > 2 AND gender IS NOT NULL
            | KEEP avg*, languages, gender, emp_no
            """);

        var subPlansResults = new HashSet<LocalRelation>();
        var subPlans = InlineJoin.firstSubPlan(plan, subPlansResults);
        var firstSubPlan = subPlans.stubReplacedSubPlan();

        var project = as(plan, EsqlProject.class);
        var limit = as(project.child(), Limit.class);

        // common filter, above first InlineJoin (inline stats ... by gender)
        var commonFilter = as(limit.child(), Filter.class);
        assertThat(commonFilter.condition(), instanceOf(GreaterThan.class));
        var commonGt = as(commonFilter.condition(), GreaterThan.class);
        var commonGtField = as(commonGt.left(), FieldAttribute.class);
        assertEquals("languages", commonGtField.name());
        var commonGtRight = as(commonGt.right(), Literal.class);
        assertEquals(2, commonGtRight.value());

        // first InlineJoin left side
        var ij = as(commonFilter.child(), InlineJoin.class);
        // IS NOT NULL filter getting pushed down past first InlineJoin
        var left = as(ij.left(), Filter.class);
        assertThat(left.condition(), instanceOf(IsNotNull.class));
        var isNotNull = as(left.condition(), IsNotNull.class);
        var isNotNullField = as(isNotNull.field(), FieldAttribute.class);
        assertEquals("gender", isNotNullField.name());

        // second InlineJoin left side
        var innerIj = as(left.child(), InlineJoin.class);
        var innerLeft = as(innerIj.left(), EsRelation.class);
        // second InlineJoin right side
        var innerRight = as(innerIj.right(), Project.class);
        // What EsqlSession is doing
        var firstSubPlanInnerRight = as(firstSubPlan, Project.class);
        assertEquals(innerRight.output(), firstSubPlanInnerRight.output());
        var innerEval = as(innerRight.child(), Eval.class);
        var firstSubPlanInnerEval = as(firstSubPlanInnerRight.child(), Eval.class);
        assertEquals(innerEval.fields(), firstSubPlanInnerEval.fields());
        var innerAggregate = as(innerEval.child(), Aggregate.class);
        var firstSubPlanInnerAggregate = as(firstSubPlanInnerEval.child(), Aggregate.class);
        assertEquals(innerAggregate.groupings(), firstSubPlanInnerAggregate.groupings());
        assertEquals(innerAggregate.aggregates(), firstSubPlanInnerAggregate.aggregates());
        var firstSubPlanInnerRelation = as(firstSubPlanInnerAggregate.child(), EsRelation.class);
        // important bit below: the EsRelation that is executed in the right hand side is the same as the one in the left hand side
        assertEquals(innerLeft, firstSubPlanInnerRelation);

        // simulate the result of the first sub-plan execution
        // not important what the actual values are, just need to have something to feed into the second InlineJoin
        List<Attribute> schema = innerRight.output(); // [avgByL{r}#5, languages{f}#20]
        Block[] blocks = new Block[schema.size()];
        blocks[0] = BlockUtils.constantBlock(
            TestBlockFactory.getNonBreakingInstance(),
            new Literal(Source.EMPTY, 5.5, DataType.DOUBLE).value(),
            1
        );
        blocks[1] = BlockUtils.constantBlock(
            TestBlockFactory.getNonBreakingInstance(),
            new Literal(Source.EMPTY, new BytesRef("M"), DataType.KEYWORD).value(),
            1
        );
        var resultWrapper = new LocalRelation(subPlans.stubReplacedSubPlan().source(), schema, LocalSupplier.of(new Page(blocks)));
        subPlansResults.add(resultWrapper);
        // this is Second InlineJoin stubReplacedSubPlan
        firstSubPlan = firstSubPlan(newMainPlan(plan, subPlans, resultWrapper), subPlansResults).stubReplacedSubPlan();

        // first InlineJoin right side
        var right = as(ij.right(), Project.class);
        // What EsqlSession is doing
        var firstSubPlanProject = as(firstSubPlan, Project.class);
        assertEquals(right.output(), firstSubPlanProject.output());
        var eval = as(right.child(), Eval.class);
        var firstSubPlanEval = as(firstSubPlanProject.child(), Eval.class);
        assertEquals(eval.fields(), firstSubPlanEval.fields());
        var aggregate = as(eval.child(), Aggregate.class);
        var firstSubPlanAggregate = as(firstSubPlanEval.child(), Aggregate.class);
        assertEquals(aggregate.groupings(), firstSubPlanAggregate.groupings());
        assertEquals(aggregate.aggregates(), firstSubPlanAggregate.aggregates());

        // the IS NOT NULL filter is the same as the one pushed down on the left side of the first InlineJoin
        var firstSubPlanFilter = as(firstSubPlanAggregate.child(), Filter.class);
        assertEquals(firstSubPlanFilter.condition(), left.condition());
    }

    /*
     * EsqlProject[[avgByL{r}#5, avgByG{r}#9, languages{f}#22, gender{f}#21, emp_no{f}#19]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[emp_no{f}#19 > 10050[INTEGER]]
     *     \_InlineJoin[LEFT,[gender{f}#21, languages{f}#22],[gender{r}#21, languages{r}#22]]
     *       |_Filter[ISNOTNULL(gender{f}#21)]
     *       | \_InlineJoin[LEFT,[languages{f}#22],[languages{r}#22]]
     *       |   |_Filter[languages{f}#22 > 2[INTEGER]]
     *       |   | \_EsRelation[employees][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     *       |   \_Project[[avgByL{r}#5, languages{f}#22]]
     *       |     \_Eval[[$$SUM$avgByL$0{r$}#31 / $$COUNT$avgByL$1{r$}#32 AS avgByL#5]]
     *       |       \_Aggregate[[languages{f}#22],[SUM(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS
     * $$SUM$avgByL$0#31, COUNT(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByL$1#32, languages{f}#22]]
     *       |         \_StubRelation[[_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, gender{f}#21, hire_date{f}#26, job{f}#27,
     * job.raw{f}#28, languages{f}#22, last_name{f}#23, long_noidx{f}#29, salary{f}#24]]
     *       \_Project[[avgByG{r}#9, gender{f}#21, languages{f}#22]]
     *         \_Eval[[$$SUM$avgByG$0{r$}#33 / $$COUNT$avgByG$1{r$}#34 AS avgByG#9]]
     *           \_Aggregate[[gender{f}#21, languages{f}#22],[SUM(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$
     * SUM$avgByG$0#33, COUNT(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByG$1#34, gender{f}#21, languages{f}#22]]
     *             \_StubRelation[[_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, gender{f}#21, hire_date{f}#26, job{f}#27,
     * job.raw{f}#28, last_name{f}#23, long_noidx{f}#29, salary{f}#24, avgByL{r}#5, languages{f}#22]]
     *
     * First InlineJoin stubReplacedSubPlan:
     * Project[[avgByL{r}#5, languages{f}#22]]
     * \_Eval[[$$SUM$avgByL$0{r$}#31 / $$COUNT$avgByL$1{r$}#32 AS avgByL#5]]
     *   \_Aggregate[[languages{f}#22],[SUM(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avgByL$0#3
     * 1, COUNT(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByL$1#32, languages{f}#22]]
     *     \_Filter[languages{f}#22 > 2[INTEGER]]
     *       \_EsRelation[employees][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     *
     * Second InlineJoin stubReplacedSubPlan:
     * Project[[avgByG{r}#9, gender{f}#21, languages{f}#22]]
     * \_Eval[[$$SUM$avgByG$0{r$}#33 / $$COUNT$avgByG$1{r$}#34 AS avgByG#9]]
     *   \_Aggregate[[gender{f}#21, languages{f}#22],[SUM(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$
     * SUM$avgByG$0#33, COUNT(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByG$1#34, gender{f}#21, languages{f}#22]]
     *     \_Filter[ISNOTNULL(gender{f}#21)]
     *       \_InlineJoin[LEFT,[languages{f}#22],[languages{r}#22]]
     *         |_Filter[languages{f}#22 > 2[INTEGER]]
     *         | \_EsRelation[employees][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     *         \_LocalRelation[[avgByL{r}#5, languages{f}#22],org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier@d78bf66e]
     */
    public void testPartiallyPushDown_GroupingFilters_PastTwoInlineJoins2() {
        var plan = plan("""
            FROM employees
            | INLINE STATS avgByL = AVG(salary) BY languages
            | INLINE STATS avgByG = AVG(salary) BY gender, languages
            | WHERE languages > 2 AND gender IS NOT NULL AND emp_no > 10050
            | KEEP avg*, languages, gender, emp_no
            """);

        // the optimized query should roughly look like this:
        /*
        FROM employees
            | WHERE languages > 2
            | INLINE STATS avgByL = AVG(salary) BY languages
            | WHERE gender IS NOT NULL
            | INLINE STATS avgByG = AVG(salary) BY gender, languages
            | WHERE emp_no > 10050
            | KEEP avg*, languages, gender, emp_no
         */
        var subPlansResults = new HashSet<LocalRelation>();
        var subPlans = InlineJoin.firstSubPlan(plan, subPlansResults);
        var firstSubPlan = subPlans.stubReplacedSubPlan();

        var project = as(plan, EsqlProject.class);
        var limit = as(project.child(), Limit.class);

        // common filter, above first InlineJoin (inline stats ... by gender, languages)
        var commonFilter = as(limit.child(), Filter.class);
        assertThat(commonFilter.condition(), instanceOf(GreaterThan.class));
        var commonGt = as(commonFilter.condition(), GreaterThan.class);
        var commonGtField = as(commonGt.left(), FieldAttribute.class);
        assertEquals("emp_no", commonGtField.name());
        var commonGtRight = as(commonGt.right(), Literal.class);
        assertEquals(10050, commonGtRight.value());

        // first InlineJoin left side
        var ij = as(commonFilter.child(), InlineJoin.class);
        // IS NOT NULL filter getting pushed down past first InlineJoin
        var left = as(ij.left(), Filter.class);
        assertThat(left.condition(), instanceOf(IsNotNull.class));
        var isNotNull = as(left.condition(), IsNotNull.class);
        var isNotNullField = as(isNotNull.field(), FieldAttribute.class);
        assertEquals("gender", isNotNullField.name());

        // second InlineJoin left side
        var innerIj = as(left.child(), InlineJoin.class);
        var innerLeft = as(innerIj.left(), Filter.class);
        assertThat(innerLeft.condition(), instanceOf(GreaterThan.class));
        var innerGt = as(innerLeft.condition(), GreaterThan.class);
        var innerGtField = as(innerGt.left(), FieldAttribute.class);
        assertEquals("languages", innerGtField.name());
        var innerGtRight = as(innerGt.right(), Literal.class);
        assertEquals(2, innerGtRight.value());
        var innerLeftChild = as(innerLeft.child(), EsRelation.class);

        // second InlineJoin right side
        var innerRight = as(innerIj.right(), Project.class);
        // What EsqlSession is doing
        var firstSubPlanInnerRight = as(firstSubPlan, Project.class);
        assertEquals(innerRight.output(), firstSubPlanInnerRight.output());
        var innerEval = as(innerRight.child(), Eval.class);
        var firstSubPlanInnerEval = as(firstSubPlanInnerRight.child(), Eval.class);
        assertEquals(innerEval.fields(), firstSubPlanInnerEval.fields());
        var innerAggregate = as(innerEval.child(), Aggregate.class);
        var firstSubPlanInnerAggregate = as(firstSubPlanInnerEval.child(), Aggregate.class);
        assertEquals(innerAggregate.groupings(), firstSubPlanInnerAggregate.groupings());
        assertEquals(innerAggregate.aggregates(), firstSubPlanInnerAggregate.aggregates());
        var firstSubPlanInnerRelation = as(firstSubPlanInnerAggregate.child(), Filter.class);
        assertThat(firstSubPlanInnerRelation.condition(), instanceOf(GreaterThan.class));
        var firstSubPlanInnerRelationGt = as(firstSubPlanInnerRelation.condition(), GreaterThan.class);
        var firstSubPlanInnerRelationGtField = as(firstSubPlanInnerRelationGt.left(), FieldAttribute.class);
        assertEquals("languages", firstSubPlanInnerRelationGtField.name());
        var firstSubPlanInnerRelationGtRight = as(firstSubPlanInnerRelationGt.right(), Literal.class);
        assertEquals(2, firstSubPlanInnerRelationGtRight.value());
        var firstSubPlanInnerRelationChild = as(firstSubPlanInnerRelation.child(), EsRelation.class);

        // important bit below: the EsRelation that is executed in the right hand side is the same as the one in the left hand side
        assertEquals(innerLeft, firstSubPlanInnerRelation);

        // simulate the result of the first sub-plan execution
        // not important what the actual values are, just need to have something to feed into the second InlineJoin
        List<Attribute> schema = innerRight.output(); // [avgByL{r}#5, languages{f}#20]
        Block[] blocks = new Block[schema.size()];
        blocks[0] = BlockUtils.constantBlock(
            TestBlockFactory.getNonBreakingInstance(),
            new Literal(Source.EMPTY, 5.5, DataType.DOUBLE).value(),
            1
        );
        blocks[1] = BlockUtils.constantBlock(
            TestBlockFactory.getNonBreakingInstance(),
            new Literal(Source.EMPTY, new BytesRef("M"), DataType.KEYWORD).value(),
            1
        );
        var resultWrapper = new LocalRelation(subPlans.stubReplacedSubPlan().source(), schema, LocalSupplier.of(new Page(blocks)));
        subPlansResults.add(resultWrapper);
        LogicalPlan newMainPlan = newMainPlan(plan, subPlans, resultWrapper);
        // this is Second InlineJoin stubReplacedSubPlan
        firstSubPlan = firstSubPlan(newMainPlan, subPlansResults).stubReplacedSubPlan();

        // first InlineJoin right side
        var right = as(ij.right(), Project.class);
        // What EsqlSession is doing
        var firstSubPlanProject = as(firstSubPlan, Project.class);
        assertEquals(right.output(), firstSubPlanProject.output());
        var eval = as(right.child(), Eval.class);
        var firstSubPlanEval = as(firstSubPlanProject.child(), Eval.class);
        assertEquals(eval.fields(), firstSubPlanEval.fields());
        var aggregate = as(eval.child(), Aggregate.class);
        var firstSubPlanAggregate = as(firstSubPlanEval.child(), Aggregate.class);
        assertEquals(aggregate.groupings(), firstSubPlanAggregate.groupings());
        assertEquals(aggregate.aggregates(), firstSubPlanAggregate.aggregates());

        // the IS NOT NULL filter is the same as the one pushed down on the left side of the first InlineJoin
        var firstSubPlanFilter = as(firstSubPlanAggregate.child(), Filter.class);
        assertEquals(firstSubPlanFilter.condition(), left.condition());
    }

    /*
     * EsqlProject[[avgByL{r}#5, avgByG{r}#9, languages{f}#22, gender{f}#21, emp_no{f}#19]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[avgByL{r}#5 > 40000[INTEGER]]
     *     \_InlineJoin[LEFT,[gender{f}#21, languages{f}#22],[gender{r}#21, languages{r}#22]]
     *       |_Filter[ISNOTNULL(gender{f}#21)]
     *       | \_InlineJoin[LEFT,[languages{f}#22],[languages{r}#22]]
     *       |   |_Filter[languages{f}#22 > 3[INTEGER]]
     *       |   | \_EsRelation[employees][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     *       |   \_Project[[avgByL{r}#5, languages{f}#22]]
     *       |     \_Eval[[$$SUM$avgByL$0{r$}#31 / $$COUNT$avgByL$1{r$}#32 AS avgByL#5]]
     *       |       \_Aggregate[[languages{f}#22],[SUM(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS
     * $$SUM$avgByL$0#31, COUNT(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByL$1#32, languages{f}#22]]
     *       |         \_StubRelation[[_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, gender{f}#21, hire_date{f}#26, job{f}#27,
     * job.raw{f}#28, languages{f}#22, last_name{f}#23, long_noidx{f}#29, salary{f}#24]]
     *       \_Project[[avgByG{r}#9, gender{f}#21, languages{f}#22]]
     *         \_Eval[[$$SUM$avgByG$0{r$}#33 / $$COUNT$avgByG$1{r$}#34 AS avgByG#9]]
     *           \_Aggregate[[gender{f}#21, languages{f}#22],[SUM(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$
     * SUM$avgByG$0#33, COUNT(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByG$1#34, gender{f}#21, languages{f}#22]]
     *             \_StubRelation[[_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, gender{f}#21, hire_date{f}#26, job{f}#27,
     * job.raw{f}#28, last_name{f}#23, long_noidx{f}#29, salary{f}#24, avgByL{r}#5, languages{f}#22]]
     *
     * First InlineJoin stubReplacedSubPlan:
     * Project[[avgByL{r}#5, languages{f}#22]]
     * \_Eval[[$$SUM$avgByL$0{r$}#31 / $$COUNT$avgByL$1{r$}#32 AS avgByL#5]]
     *   \_Aggregate[[languages{f}#22],[SUM(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avgByL$0#3
     * 1, COUNT(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByL$1#32, languages{f}#22]]
     *     \_Filter[languages{f}#22 > 3[INTEGER]]
     *       \_EsRelation[employees][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     *
     * Second InlineJoin stubReplacedSubPlan:
     * Project[[avgByG{r}#9, gender{f}#21, languages{f}#22]]
     * \_Eval[[$$SUM$avgByG$0{r$}#33 / $$COUNT$avgByG$1{r$}#34 AS avgByG#9]]
     *   \_Aggregate[[gender{f}#21, languages{f}#22],[SUM(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$
     * SUM$avgByG$0#33, COUNT(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByG$1#34, gender{f}#21, languages{f}#22]]
     *     \_Filter[ISNOTNULL(gender{f}#21)]
     *       \_InlineJoin[LEFT,[languages{f}#22],[languages{r}#22]]
     *         |_Filter[languages{f}#22 > 3[INTEGER]]
     *         | \_EsRelation[employees][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     *         \_LocalRelation[[avgByL{r}#5, languages{f}#22],org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier@1dc60410]
     */
    public void testPartiallyPushDown_GroupingFilters_PastTwoInlineJoins_ExcludeAggFilters() {
        var plan = plan("""
            FROM employees
            | INLINE STATS avgByL = AVG(salary) BY languages
            | INLINE STATS avgByG = AVG(salary) BY gender, languages
            | WHERE languages > 3 AND gender IS NOT NULL AND avgByL > 40000
            | KEEP avgB*, languages, gender, emp_no
            """);

        // the optimized query should roughly look like this:
        /*
        FROM employees
            | WHERE languages > 3
            | INLINE STATS avgByL = AVG(salary) BY languages
            | WHERE gender IS NOT NULL
            | INLINE STATS avgByG = AVG(salary) BY gender, languages
            | WHERE avgByL > 40000
            | KEEP avgB*, languages, gender, emp_no
         */
        var subPlansResults = new HashSet<LocalRelation>();
        var subPlans = InlineJoin.firstSubPlan(plan, subPlansResults);
        var firstSubPlan = subPlans.stubReplacedSubPlan();

        var project = as(plan, EsqlProject.class);
        var limit = as(project.child(), Limit.class);

        // common filter, above first InlineJoin (inline stats ... by gender, languages)
        var commonFilter = as(limit.child(), Filter.class);
        assertThat(commonFilter.condition(), instanceOf(GreaterThan.class));
        var commonGt = as(commonFilter.condition(), GreaterThan.class);
        var commonGtField = as(commonGt.left(), ReferenceAttribute.class);
        assertEquals("avgByL", commonGtField.name());
        var commonGtRight = as(commonGt.right(), Literal.class);
        assertEquals(40000, commonGtRight.value());

        // first InlineJoin left side
        var ij = as(commonFilter.child(), InlineJoin.class);
        // IS NOT NULL filter getting pushed down past first InlineJoin
        var left = as(ij.left(), Filter.class);
        assertThat(left.condition(), instanceOf(IsNotNull.class));
        var isNotNull = as(left.condition(), IsNotNull.class);
        var isNotNullField = as(isNotNull.field(), FieldAttribute.class);
        assertEquals("gender", isNotNullField.name());

        // second InlineJoin left side
        var innerIj = as(left.child(), InlineJoin.class);
        var innerLeft = as(innerIj.left(), Filter.class);
        assertThat(innerLeft.condition(), instanceOf(GreaterThan.class));
        var innerGt = as(innerLeft.condition(), GreaterThan.class);
        var innerGtField = as(innerGt.left(), FieldAttribute.class);
        assertEquals("languages", innerGtField.name());
        var innerGtRight = as(innerGt.right(), Literal.class);
        assertEquals(3, innerGtRight.value());
        var innerLeftChild = as(innerLeft.child(), EsRelation.class);

        // second InlineJoin right side
        var innerRight = as(innerIj.right(), Project.class);
        // What EsqlSession is doing
        var firstSubPlanInnerRight = as(firstSubPlan, Project.class);
        assertEquals(innerRight.output(), firstSubPlanInnerRight.output());
        var innerEval = as(innerRight.child(), Eval.class);
        var firstSubPlanInnerEval = as(firstSubPlanInnerRight.child(), Eval.class);
        assertEquals(innerEval.fields(), firstSubPlanInnerEval.fields());
        var innerAggregate = as(innerEval.child(), Aggregate.class);
        var firstSubPlanInnerAggregate = as(firstSubPlanInnerEval.child(), Aggregate.class);
        assertEquals(innerAggregate.groupings(), firstSubPlanInnerAggregate.groupings());
        assertEquals(innerAggregate.aggregates(), firstSubPlanInnerAggregate.aggregates());
        var firstSubPlanInnerRelation = as(firstSubPlanInnerAggregate.child(), Filter.class);
        assertThat(firstSubPlanInnerRelation.condition(), instanceOf(GreaterThan.class));
        var firstSubPlanInnerRelationGt = as(firstSubPlanInnerRelation.condition(), GreaterThan.class);
        var firstSubPlanInnerRelationGtField = as(firstSubPlanInnerRelationGt.left(), FieldAttribute.class);
        assertEquals("languages", firstSubPlanInnerRelationGtField.name());
        var firstSubPlanInnerRelationGtRight = as(firstSubPlanInnerRelationGt.right(), Literal.class);
        assertEquals(3, firstSubPlanInnerRelationGtRight.value());
        var firstSubPlanInnerRelationChild = as(firstSubPlanInnerRelation.child(), EsRelation.class);

        // important bit below: the EsRelation that is executed in the right hand side is the same as the one in the left hand side
        assertEquals(innerLeft, firstSubPlanInnerRelation);

        // simulate the result of the first sub-plan execution
        // not important what the actual values are, just need to have something to feed into the second InlineJoin
        List<Attribute> schema = innerRight.output(); // [avgByL{r}#5, languages{f}#20]
        Block[] blocks = new Block[schema.size()];
        blocks[0] = BlockUtils.constantBlock(
            TestBlockFactory.getNonBreakingInstance(),
            new Literal(Source.EMPTY, 5.5, DataType.DOUBLE).value(),
            1
        );
        blocks[1] = BlockUtils.constantBlock(
            TestBlockFactory.getNonBreakingInstance(),
            new Literal(Source.EMPTY, new BytesRef("M"), DataType.KEYWORD).value(),
            1
        );
        var resultWrapper = new LocalRelation(subPlans.stubReplacedSubPlan().source(), schema, LocalSupplier.of(new Page(blocks)));
        subPlansResults.add(resultWrapper);
        LogicalPlan newMainPlan = newMainPlan(plan, subPlans, resultWrapper);
        // this is Second InlineJoin stubReplacedSubPlan
        firstSubPlan = firstSubPlan(newMainPlan, subPlansResults).stubReplacedSubPlan();

        // first InlineJoin right side
        var right = as(ij.right(), Project.class);
        // What EsqlSession is doing
        var firstSubPlanProject = as(firstSubPlan, Project.class);
        assertEquals(right.output(), firstSubPlanProject.output());
        var eval = as(right.child(), Eval.class);
        var firstSubPlanEval = as(firstSubPlanProject.child(), Eval.class);
        assertEquals(eval.fields(), firstSubPlanEval.fields());
        var aggregate = as(eval.child(), Aggregate.class);
        var firstSubPlanAggregate = as(firstSubPlanEval.child(), Aggregate.class);
        assertEquals(aggregate.groupings(), firstSubPlanAggregate.groupings());
        assertEquals(aggregate.aggregates(), firstSubPlanAggregate.aggregates());

        // the IS NOT NULL filter is the same as the one pushed down on the left side of the first InlineJoin
        var firstSubPlanFilter = as(firstSubPlanAggregate.child(), Filter.class);
        assertEquals(firstSubPlanFilter.condition(), left.condition());
    }

    /*
     * EsqlProject[[avgByL{r}#9, avgByG{r}#16, lang{r}#13, gender{f}#28, emp_no{f}#26]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[emp_no{f}#26 > 10050[INTEGER]]
     *     \_InlineJoin[LEFT,[gender{f}#28, lang{r}#13],[gender{r}#28, lang{r}#13]]
     *       |_EsqlProject[[salary{f}#31, gender{f}#28, emp_no{f}#26, avgByL{r}#9, languages{f}#29 AS lang#13]]
     *       | \_Filter[ISNOTNULL(gender{f}#28)]
     *       |   \_InlineJoin[LEFT,[languages{f}#29],[languages{r}#29]]
     *       |     |_EsqlProject[[languages{f}#29, salary{f}#31, gender{f}#28, emp_no{f}#26]]
     *       |     | \_Filter[languages{f}#29 > 3[INTEGER]]
     *       |     |   \_EsRelation[employees][_meta_field{f}#32, emp_no{f}#26, first_name{f}#27, ..]
     *       |     \_Project[[avgByL{r}#9, languages{f}#29]]
     *       |       \_Eval[[$$SUM$avgByL$0{r$}#38 / $$COUNT$avgByL$1{r$}#39 AS avgByL#9]]
     *       |         \_Aggregate[[languages{f}#29],[SUM(salary{f}#31,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS
     * $$SUM$avgByL$0#38, COUNT(salary{f}#31,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByL$1#39, languages{f}#29]]
     *       |           \_StubRelation[[languages{f}#29, salary{f}#31, gender{f}#28, emp_no{f}#26]]
     *       \_Project[[avgByG{r}#16, gender{f}#28, lang{r}#13]]
     *         \_Eval[[$$SUM$avgByG$0{r$}#40 / $$COUNT$avgByG$1{r$}#41 AS avgByG#16]]
     *           \_Aggregate[[gender{f}#28, lang{r}#13],[SUM(salary{f}#31,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS
     * $$SUM$avgByG$0#40, COUNT(salary{f}#31,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByG$1#41, gender{f}#28, lang{r}#13]]
     *             \_StubRelation[[salary{f}#31, gender{f}#28, emp_no{f}#26, avgByL{r}#9, lang{r}#13]]
     *
     * First InlineJoin stubReplacedSubPlan:
     * Project[[avgByL{r}#9, languages{f}#29]]
     * \_Eval[[$$SUM$avgByL$0{r$}#38 / $$COUNT$avgByL$1{r$}#39 AS avgByL#9]]
     *   \_Aggregate[[languages{f}#29],[SUM(salary{f}#31,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS
     * $$SUM$avgByL$0#38, COUNT(salary{f}#31,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByL$1#39, languages{f}#29]]
     *     \_EsqlProject[[languages{f}#29, salary{f}#31, gender{f}#28, emp_no{f}#26]]
     *       \_Filter[languages{f}#29 > 3[INTEGER]]
     *         \_EsRelation[employees][_meta_field{f}#32, emp_no{f}#26, first_name{f}#27, ..]
     *
     * Second InlineJoin stubReplacedSubPlan:\Project[[avgByG{r}#16, gender{f}#28, lang{r}#13]]
     * \_Eval[[$$SUM$avgByG$0{r$}#40 / $$COUNT$avgByG$1{r$}#41 AS avgByG#16]]
     *   \_Aggregate[[gender{f}#28, lang{r}#13],[SUM(salary{f}#31,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$a
     * vgByG$0#40, COUNT(salary{f}#31,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByG$1#41, gender{f}#28, lang{r}#13]]
     *     \_EsqlProject[[salary{f}#31, gender{f}#28, emp_no{f}#26, avgByL{r}#9, languages{f}#29 AS lang#13]]
     *       \_Filter[ISNOTNULL(gender{f}#28)]
     *         \_InlineJoin[LEFT,[languages{f}#29],[languages{r}#29]]
     *           |_EsqlProject[[languages{f}#29, salary{f}#31, gender{f}#28, emp_no{f}#26]]
     *           | \_Filter[languages{f}#29 > 3[INTEGER]]
     *           |   \_EsRelation[employees][_meta_field{f}#32, emp_no{f}#26, first_name{f}#27, ..]
     *           \_LocalRelation[[avgByL{r}#9, languages{f}#29],org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier]
     */
    public void testPartiallyPushDown_RenamedGroupingFilters_PastTwoInlineJoins() {
        var plan = plan("""
            FROM employees
            | KEEP languages, salary, gender, emp_no
            | INLINE STATS avgByL = AVG(salary) BY languages
            | RENAME languages AS lang
            | INLINE STATS avgByG = AVG(salary) BY gender, lang
            | WHERE lang > 3 AND gender IS NOT NULL AND emp_no > 10050
            | KEEP avgB*, lang, gender, emp_no
            """);

        // the optimized query should roughly look like this:
        /*
        FROM employees
            | WHERE languages > 3
            | INLINE STATS avgByL = AVG(salary) BY languages
            | WHERE gender IS NOT NULL
            | INLINE STATS avgByG = AVG(salary) BY gender, lang
            | WHERE emp_no > 10050
            | KEEP avgB*, lang, gender, emp_no
         */
        var subPlansResults = new HashSet<LocalRelation>();
        var subPlans = InlineJoin.firstSubPlan(plan, subPlansResults);
        var firstSubPlan = subPlans.stubReplacedSubPlan();

        var project = as(plan, EsqlProject.class);
        var limit = as(project.child(), Limit.class);

        // common filter, above first InlineJoin (inline stats ... by gender, languages)
        var commonFilter = as(limit.child(), Filter.class);
        assertThat(commonFilter.condition(), instanceOf(GreaterThan.class));
        var commonGt = as(commonFilter.condition(), GreaterThan.class);
        var commonGtField = as(commonGt.left(), FieldAttribute.class);
        assertEquals("emp_no", commonGtField.name());
        var commonGtRight = as(commonGt.right(), Literal.class);
        assertEquals(10050, commonGtRight.value());

        // first InlineJoin left side
        var ij = as(commonFilter.child(), InlineJoin.class);
        // this is the projection that renames languages AS lang
        var left = as(ij.left(), EsqlProject.class);
        var projections = left.projections();
        assertThat(Expressions.names(projections), contains("salary", "gender", "emp_no", "avgByL", "lang"));
        var langRename = as(projections.get(4), Alias.class);
        assertThat(Expressions.name(langRename), containsString("lang"));
        var langField = as(langRename.child(), FieldAttribute.class);
        assertEquals("languages", langField.name());

        // IS NOT NULL filter getting pushed down past first InlineJoin
        var leftFilter = as(left.child(), Filter.class);
        assertThat(leftFilter.condition(), instanceOf(IsNotNull.class));
        var isNotNull = as(leftFilter.condition(), IsNotNull.class);
        var isNotNullField = as(isNotNull.field(), FieldAttribute.class);
        assertEquals("gender", isNotNullField.name());

        // second InlineJoin left side
        var innerIj = as(leftFilter.child(), InlineJoin.class);
        var innerLeft = as(innerIj.left(), EsqlProject.class);
        var innerProjections = innerLeft.projections();
        assertThat(Expressions.names(innerProjections), contains("languages", "salary", "gender", "emp_no"));

        var innerLeftFilter = as(innerLeft.child(), Filter.class);
        assertThat(innerLeftFilter.condition(), instanceOf(GreaterThan.class));
        var innerGt = as(innerLeftFilter.condition(), GreaterThan.class);
        var innerGtField = as(innerGt.left(), FieldAttribute.class);
        assertEquals("languages", innerGtField.name());
        var innerGtRight = as(innerGt.right(), Literal.class);
        assertEquals(3, innerGtRight.value());
        var innerLeftChild = as(innerLeftFilter.child(), EsRelation.class);

        // second InlineJoin right side
        var innerRight = as(innerIj.right(), Project.class);
        // What EsqlSession is doing
        var firstSubPlanInnerRight = as(firstSubPlan, Project.class);
        assertEquals(innerRight.output(), firstSubPlanInnerRight.output());
        var innerEval = as(innerRight.child(), Eval.class);
        var firstSubPlanInnerEval = as(firstSubPlanInnerRight.child(), Eval.class);
        assertEquals(innerEval.fields(), firstSubPlanInnerEval.fields());
        var innerAggregate = as(innerEval.child(), Aggregate.class);
        var firstSubPlanInnerAggregate = as(firstSubPlanInnerEval.child(), Aggregate.class);
        assertEquals(innerAggregate.groupings(), firstSubPlanInnerAggregate.groupings());
        assertEquals(innerAggregate.aggregates(), firstSubPlanInnerAggregate.aggregates());

        var firstSubPlanInnerRelation = as(firstSubPlanInnerAggregate.child(), EsqlProject.class);
        var firstSubPlanInnerProjections = firstSubPlanInnerRelation.projections();
        assertThat(Expressions.names(firstSubPlanInnerProjections), contains("languages", "salary", "gender", "emp_no"));

        var firstSubPlanInnerFilter = as(firstSubPlanInnerRelation.child(), Filter.class);
        assertThat(firstSubPlanInnerFilter.condition(), instanceOf(GreaterThan.class));
        var firstSubPlanInnerRelationGt = as(firstSubPlanInnerFilter.condition(), GreaterThan.class);
        var firstSubPlanInnerRelationGtField = as(firstSubPlanInnerRelationGt.left(), FieldAttribute.class);
        assertEquals("languages", firstSubPlanInnerRelationGtField.name());
        var firstSubPlanInnerRelationGtRight = as(firstSubPlanInnerRelationGt.right(), Literal.class);
        assertEquals(3, firstSubPlanInnerRelationGtRight.value());
        var firstSubPlanInnerRelationChild = as(firstSubPlanInnerFilter.child(), EsRelation.class);

        // important bit below: the EsRelation that is executed in the right hand side is the same as the one in the left hand side
        assertEquals(innerLeftFilter, firstSubPlanInnerFilter);

        // simulate the result of the first sub-plan execution
        // not important what the actual values are, just need to have something to feed into the second InlineJoin
        List<Attribute> schema = innerRight.output(); // [avgByL{r}#5, languages{f}#20]
        Block[] blocks = new Block[schema.size()];
        blocks[0] = BlockUtils.constantBlock(
            TestBlockFactory.getNonBreakingInstance(),
            new Literal(Source.EMPTY, 5.5, DataType.DOUBLE).value(),
            1
        );
        blocks[1] = BlockUtils.constantBlock(
            TestBlockFactory.getNonBreakingInstance(),
            new Literal(Source.EMPTY, new BytesRef("M"), DataType.KEYWORD).value(),
            1
        );
        var resultWrapper = new LocalRelation(subPlans.stubReplacedSubPlan().source(), schema, LocalSupplier.of(new Page(blocks)));
        subPlansResults.add(resultWrapper);
        LogicalPlan newMainPlan = newMainPlan(plan, subPlans, resultWrapper);
        // this is Second InlineJoin stubReplacedSubPlan
        firstSubPlan = firstSubPlan(newMainPlan, subPlansResults).stubReplacedSubPlan();

        // first InlineJoin right side
        var right = as(ij.right(), Project.class);
        // What EsqlSession is doing
        var firstSubPlanProject = as(firstSubPlan, Project.class);
        assertEquals(right.output(), firstSubPlanProject.output());
        var eval = as(right.child(), Eval.class);
        var firstSubPlanEval = as(firstSubPlanProject.child(), Eval.class);
        assertEquals(eval.fields(), firstSubPlanEval.fields());
        var aggregate = as(eval.child(), Aggregate.class);
        var firstSubPlanAggregate = as(firstSubPlanEval.child(), Aggregate.class);
        assertEquals(aggregate.groupings(), firstSubPlanAggregate.groupings());
        assertEquals(aggregate.aggregates(), firstSubPlanAggregate.aggregates());

        var firstSubPlanProjection = as(ij.left(), EsqlProject.class);
        projections = firstSubPlanProjection.projections();
        assertThat(Expressions.names(projections), contains("salary", "gender", "emp_no", "avgByL", "lang"));
        langRename = as(projections.get(4), Alias.class);
        assertThat(Expressions.name(langRename), containsString("lang"));
        langField = as(langRename.child(), FieldAttribute.class);
        assertEquals("languages", langField.name());
        // the IS NOT NULL filter is the same as the one pushed down on the left side of the first InlineJoin
        var firstSubPlanFilter = as(firstSubPlanProjection.child(), Filter.class);
        assertEquals(firstSubPlanFilter.condition(), leftFilter.condition());
    }

    /*
     * EsqlProject[[a{r}#5, gender{f}#12]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[a{r}#5 > 55000[INTEGER]]
     *     \_InlineJoin[LEFT,[gender{f}#12],[gender{r}#12]]
     *       |_EsRelation[employees][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     *       \_Project[[a{r}#5, gender{f}#12]]
     *         \_Eval[[$$SUM$a$0{r$}#21 / $$COUNT$a$1{r$}#22 AS a#5]]
     *           \_Aggregate[[gender{f}#12],[SUM(salary{f}#15,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$a$0#21,
     * COUNT(salary{f}#15,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$a$1#22, gender{f}#12]]
     *             \_StubRelation[[_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, gender{f}#12, hire_date{f}#17, job{f}#18,
     * job.raw{f}#19, languages{f}#13, last_name{f}#14, long_noidx{f}#20, salary{f}#15]]
     */
    public void testDontPushDown_OnRightSideOf_InlineStats() {
        // a > 55000 even if it makes sense to have it pushed down on the right hand side, atm it cannot be done for inlinestats
        var plan = plan("""
            FROM employees
            | INLINE STATS a = AVG(salary) BY gender
            | WHERE a > 55000
            | KEEP a, gender
            """);

        var project = as(plan, EsqlProject.class);
        var limit = as(project.child(), Limit.class);
        // common filter, above InlineJoin
        var commonFilter = as(limit.child(), Filter.class);
        assertThat(commonFilter.condition(), instanceOf(GreaterThan.class));
        var commonGt = as(commonFilter.condition(), GreaterThan.class);
        var commonGtField = as(commonGt.left(), ReferenceAttribute.class);
        assertEquals("a", commonGtField.name());
        var commonGtRight = as(commonGt.right(), Literal.class);
        assertEquals(55000, commonGtRight.value());

        // InlineJoin
        var ij = as(commonFilter.child(), InlineJoin.class);
        // InlineJoin left side
        var left = as(ij.left(), EsRelation.class);
        // InlineJoin right side
        var right = as(ij.right(), Project.class);
        var eval = as(right.child(), Eval.class);
        var aggregate = as(eval.child(), Aggregate.class);
        as(aggregate.child(), StubRelation.class);
    }

    /*
     * EsqlProject[[avgByL{r}#5, avgByG{r}#9, languages{f}#22, gender{f}#21, emp_no{f}#19]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[languages{f}#22 > 3[INTEGER] AND avgByL{r}#5 > 40000[INTEGER] AND avgByG{r}#9 < 50000[INTEGER]]
     *     \_InlineJoin[LEFT,[gender{f}#21],[gender{r}#21]]
     *       |_Filter[ISNOTNULL(gender{f}#21)]
     *       | \_InlineJoin[LEFT,[languages{f}#22],[languages{r}#22]]
     *       |   |_EsRelation[employees][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     *       |   \_Project[[avgByL{r}#5, languages{f}#22]]
     *       |     \_Eval[[$$SUM$avgByL$0{r$}#31 / $$COUNT$avgByL$1{r$}#32 AS avgByL#5]]
     *       |       \_Aggregate[[languages{f}#22],[SUM(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS
     * $$SUM$avgByL$0#31, COUNT(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByL$1#32, languages{f}#22]]
     *       |         \_StubRelation[[_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, gender{f}#21, hire_date{f}#26, job{f}#27,
     * job.raw{f}#28, languages{f}#22, last_name{f}#23, long_noidx{f}#29, salary{f}#24]]
     *       \_Project[[avgByG{r}#9, gender{f}#21]]
     *         \_Eval[[$$SUM$avgByG$0{r$}#33 / $$COUNT$avgByG$1{r$}#34 AS avgByG#9]]
     *           \_Aggregate[[gender{f}#21],[SUM(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avgByG$0#33,
     * COUNT(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByG$1#34, gender{f}#21]]
     *             \_StubRelation[[_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, gender{f}#21, hire_date{f}#26, job{f}#27,
     * job.raw{f}#28, last_name{f}#23, long_noidx{f}#29, salary{f}#24, avgByL{r}#5, languages{f}#22]]
     *
     * First InlineJoin stubReplacedSubPlan:
     * Project[[avgByL{r}#5, languages{f}#22]]
     * \_Eval[[$$SUM$avgByL$0{r$}#31 / $$COUNT$avgByL$1{r$}#32 AS avgByL#5]]
     *   \_Aggregate[[languages{f}#22],[SUM(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avgByL$0#3
     * 1, COUNT(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByL$1#32, languages{f}#22]]
     *     \_EsRelation[employees][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     *
     * Second InlineJoin stubReplacedSubPlan:
     * Project[[avgByG{r}#9, gender{f}#21]]
     * \_Eval[[$$SUM$avgByG$0{r$}#33 / $$COUNT$avgByG$1{r$}#34 AS avgByG#9]]
     *   \_Aggregate[[gender{f}#21],[SUM(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avgByG$0#33,
     * COUNT(salary{f}#24,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgByG$1#34, gender{f}#21]]
     *     \_Filter[ISNOTNULL(gender{f}#21)]
     *       \_InlineJoin[LEFT,[languages{f}#22],[languages{r}#22]]
     *         |_EsRelation[employees][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     *         \_LocalRelation[[avgByL{r}#5, languages{f}#22],org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier@817a68ce]
     */
    public void testPartiallyPushDown_GroupingFilters_PastTwoInlineJoins_ExcludeComplexAggFilters() {
        var plan = plan("""
            FROM employees
            | INLINE STATS avgByL = AVG(salary) BY languages
            | INLINE STATS avgByG = AVG(salary) BY gender
            | WHERE languages > 3 AND gender IS NOT NULL AND avgByL > 40000 AND avgByG < 50000
            | KEEP avgB*, languages, gender, emp_no
            """);

        // the optimized query should roughly look like this:
        /*
        FROM employees
            | INLINE STATS avgByL = AVG(salary) BY languages
            | WHERE gender IS NOT NULL
            | INLINE STATS avgByG = AVG(salary) BY gender
            | WHERE languages > 3 AND avgByL > 40000 AND avgByG < 50000
            | KEEP avgB*, languages, gender, emp_no
         */
        var subPlansResults = new HashSet<LocalRelation>();
        var subPlans = InlineJoin.firstSubPlan(plan, subPlansResults);
        var firstSubPlan = subPlans.stubReplacedSubPlan();

        var project = as(plan, EsqlProject.class);
        var limit = as(project.child(), Limit.class);

        // common filter, above first InlineJoin (inline stats ... by languages)
        var commonFilter = as(limit.child(), Filter.class);
        assertThat(commonFilter.condition(), instanceOf(And.class));
        var and = as(commonFilter.condition(), And.class);
        var andRight = as(and.right(), LessThan.class);
        var andRightField = as(andRight.left(), ReferenceAttribute.class);
        assertEquals("avgByG", andRightField.name());
        var andRightRight = as(andRight.right(), Literal.class);
        assertEquals(50000, andRightRight.value());
        var andLeft = as(and.left(), And.class);
        var andLeftLeft = as(andLeft.left(), GreaterThan.class);
        var andLeftLeftField = as(andLeftLeft.left(), FieldAttribute.class);
        assertEquals("languages", andLeftLeftField.name());
        var andLeftLeftRight = as(andLeftLeft.right(), Literal.class);
        assertEquals(3, andLeftLeftRight.value());
        var andLeftRight = as(andLeft.right(), GreaterThan.class);
        var andLeftRightField = as(andLeftRight.left(), ReferenceAttribute.class);
        assertEquals("avgByL", andLeftRightField.name());
        var andLeftRightRight = as(andLeftRight.right(), Literal.class);
        assertEquals(40000, andLeftRightRight.value());

        // first InlineJoin left side
        var ij = as(commonFilter.child(), InlineJoin.class);
        // IS NOT NULL filter getting pushed down past first InlineJoin
        var left = as(ij.left(), Filter.class);
        assertThat(left.condition(), instanceOf(IsNotNull.class));
        var isNotNull = as(left.condition(), IsNotNull.class);
        var isNotNullField = as(isNotNull.field(), FieldAttribute.class);
        assertEquals("gender", isNotNullField.name());

        // second InlineJoin left side
        var innerIj = as(left.child(), InlineJoin.class);
        var innerLeft = as(innerIj.left(), EsRelation.class);

        // second InlineJoin right side
        var innerRight = as(innerIj.right(), Project.class);
        // What EsqlSession is doing
        var firstSubPlanInnerRight = as(firstSubPlan, Project.class);
        assertEquals(innerRight.output(), firstSubPlanInnerRight.output());
        var innerEval = as(innerRight.child(), Eval.class);
        var firstSubPlanInnerEval = as(firstSubPlanInnerRight.child(), Eval.class);
        assertEquals(innerEval.fields(), firstSubPlanInnerEval.fields());
        var innerAggregate = as(innerEval.child(), Aggregate.class);
        var firstSubPlanInnerAggregate = as(firstSubPlanInnerEval.child(), Aggregate.class);
        assertEquals(innerAggregate.groupings(), firstSubPlanInnerAggregate.groupings());
        assertEquals(innerAggregate.aggregates(), firstSubPlanInnerAggregate.aggregates());
        var firstSubPlanInnerRelation = as(firstSubPlanInnerAggregate.child(), EsRelation.class);

        // important bit below: the EsRelation that is executed in the right hand side is the same as the one in the left hand side
        assertEquals(innerLeft, firstSubPlanInnerRelation);

        // simulate the result of the first sub-plan execution
        // not important what the actual values are, just need to have something to feed into the second InlineJoin
        List<Attribute> schema = innerRight.output(); // [avgByL{r}#5, languages{f}#22]
        Block[] blocks = new Block[schema.size()];
        blocks[0] = BlockUtils.constantBlock(
            TestBlockFactory.getNonBreakingInstance(),
            new Literal(Source.EMPTY, 5.5, DataType.DOUBLE).value(),
            1
        );
        blocks[1] = BlockUtils.constantBlock(
            TestBlockFactory.getNonBreakingInstance(),
            new Literal(Source.EMPTY, new BytesRef("M"), DataType.KEYWORD).value(),
            1
        );
        var resultWrapper = new LocalRelation(subPlans.stubReplacedSubPlan().source(), schema, LocalSupplier.of(new Page(blocks)));
        subPlansResults.add(resultWrapper);
        LogicalPlan newMainPlan = newMainPlan(plan, subPlans, resultWrapper);
        // this is Second InlineJoin stubReplacedSubPlan
        firstSubPlan = firstSubPlan(newMainPlan, subPlansResults).stubReplacedSubPlan();

        // first InlineJoin right side
        var right = as(ij.right(), Project.class);
        // What EsqlSession is doing
        var firstSubPlanProject = as(firstSubPlan, Project.class);
        assertEquals(right.output(), firstSubPlanProject.output());
        var eval = as(right.child(), Eval.class);
        var firstSubPlanEval = as(firstSubPlanProject.child(), Eval.class);
        assertEquals(eval.fields(), firstSubPlanEval.fields());
        var aggregate = as(eval.child(), Aggregate.class);
        var firstSubPlanAggregate = as(firstSubPlanEval.child(), Aggregate.class);
        assertEquals(aggregate.groupings(), firstSubPlanAggregate.groupings());
        assertEquals(aggregate.aggregates(), firstSubPlanAggregate.aggregates());

        // the IS NOT NULL filter is the same as the one pushed down on the left side of the first InlineJoin
        var firstSubPlanFilter = as(firstSubPlanAggregate.child(), Filter.class);
        assertEquals(firstSubPlanFilter.condition(), left.condition());
    }

    /*
     * EsqlProject[[sum{r}#5, languages{f}#15, salary{f}#17]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_InlineJoin[LEFT,[languages{f}#15],[languages{r}#15]]
     *     |_Filter[languages{f}#15 > 2[INTEGER]]
     *     | \_EsRelation[employees][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     *     \_Aggregate[[languages{f}#15],[SUM(salary{f}#17,salary{f}#17 > 50000[INTEGER],PT0S[TIME_DURATION],compensated[KEYWORD]) AS
     * sum#5, languages{f}#15]]
     *       \_StubRelation[[_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, gender{f}#14, hire_date{f}#19, job{f}#20, job.raw{f}#21, l
     * anguages{f}#15, last_name{f}#16, long_noidx{f}#22, salary{f}#17]]
     *
     * stubReplacedSubPlan:
     * Aggregate[[languages{f}#15],[SUM(salary{f}#17,salary{f}#17 > 50000[INTEGER],PT0S[TIME_DURATION],compensated[KEYWORD]) AS
     * sum#5, languages{f}#15]]
     * \_Filter[languages{f}#15 > 2[INTEGER]]
     *   \_EsRelation[employees][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     */
    public void testPushDown_OneGroupingFilter_PastInlineJoinWithInnerFilter() {
        var plan = plan("""
            FROM employees
            | INLINE STATS sum = SUM(salary) WHERE salary > 50000  BY languages
            | WHERE languages > 2
            | KEEP sum, languages, salary
            """);

        var subPlansResults = new HashSet<LocalRelation>();
        var firstSubPlan = InlineJoin.firstSubPlan(plan, subPlansResults).stubReplacedSubPlan();

        var project = as(plan, EsqlProject.class);
        var limit = as(project.child(), Limit.class);

        // InlineJoin left side
        var ij = as(limit.child(), InlineJoin.class);
        var left = as(ij.left(), Filter.class);
        assertThat(left.condition(), instanceOf(GreaterThan.class));
        var gt = as(left.condition(), GreaterThan.class);
        var fieldAttr = as(gt.left(), FieldAttribute.class);
        assertEquals("languages", fieldAttr.name());
        var gtRight = as(gt.right(), Literal.class);
        assertEquals(2, gtRight.value());
        as(left.child(), EsRelation.class);

        // InlineJoin right side
        var right = as(ij.right(), Aggregate.class);
        as(right.child(), StubRelation.class);

        // What EsqlSession is doing
        var firstSubPlanAggregate = as(firstSubPlan, Aggregate.class);
        assertEquals(right.groupings(), firstSubPlanAggregate.groupings());
        assertEquals(right.aggregates(), firstSubPlanAggregate.aggregates());

        // and somewhat the essential part is that the filter is being kept on the AggregateFunction, and not extracted out
        // this is being handled in AbstractPhysicalOperationProviders.aggregatesToFactory
        var aggregates = right.aggregates();
        assertThat(aggregates.size(), is(2));

        assertThat(aggregates.get(0), instanceOf(Alias.class));
        var alias = as(aggregates.get(0), Alias.class);
        var sum = as(alias.child(), Sum.class);
        var sumFilter = as(sum.filter(), GreaterThan.class);
        var sumFilterField = as(sumFilter.left(), FieldAttribute.class);
        assertEquals("salary", sumFilterField.name());
        var sumFilterRight = as(sumFilter.right(), Literal.class);
        assertEquals(50000, sumFilterRight.value());

        assertThat(aggregates.get(1), instanceOf(FieldAttribute.class));
        var langField = as(aggregates.get(1), FieldAttribute.class);
        assertEquals("languages", langField.name());

        var firstSubPlanFilter = as(firstSubPlanAggregate.child(), Filter.class);
        // important bit below: the filter that is executed in the right hand side is the same as the one in the left hand side
        assertEquals(left, firstSubPlanFilter);
    }
}
