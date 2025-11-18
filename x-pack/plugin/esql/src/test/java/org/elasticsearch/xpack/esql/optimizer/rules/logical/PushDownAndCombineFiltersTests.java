/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
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
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;

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
}
