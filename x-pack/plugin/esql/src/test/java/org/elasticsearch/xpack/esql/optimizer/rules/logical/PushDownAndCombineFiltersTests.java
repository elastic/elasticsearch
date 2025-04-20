/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.FOUR;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.ONE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.THREE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOrEqualOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.rlike;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.wildcardLike;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.mockito.Mockito.mock;

public class PushDownAndCombineFiltersTests extends ESTestCase {

    public void testCombineFilters() {
        EsRelation relation = relation();
        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);

        Filter fa = new Filter(EMPTY, relation, conditionA);
        Filter fb = new Filter(EMPTY, fa, conditionB);

        assertEquals(new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)), new PushDownAndCombineFilters().apply(fb));
    }

    public void testCombineFiltersLikeRLike() {
        EsRelation relation = relation();
        RLike conditionA = rlike(getFieldAttribute("a"), "foo");
        WildcardLike conditionB = wildcardLike(getFieldAttribute("b"), "bar");

        Filter fa = new Filter(EMPTY, relation, conditionA);
        Filter fb = new Filter(EMPTY, fa, conditionB);

        assertEquals(new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)), new PushDownAndCombineFilters().apply(fb));
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
        assertEquals(new EsqlProject(EMPTY, combinedFilter, projections), new PushDownAndCombineFilters().apply(fb));
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

        LogicalPlan optimized = new PushDownAndCombineFilters().apply(filter);

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

                LogicalPlan plan = new PushDownAndCombineFilters().apply(filter);

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
        assertEquals(new EsqlProject(EMPTY, combinedFilter, projections), new PushDownAndCombineFilters().apply(fb));
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
        assertEquals(expected, new PushDownAndCombineFilters().apply(fb));
    }

    // from ... | where a > 1 | COMPLETION "some prompt" WITH reranker AS completion | where b < 2 and match(completion, some text)
    // => ... | where a > 1 AND b < 2| COMPLETION "some prompt" WITH reranker AS completion | match(completion, some text)
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

        assertEquals(expectedOptimizedPlan, new PushDownAndCombineFilters().apply(filterB));
    }

    private static Completion completion(LogicalPlan child) {
        return new Completion(
            EMPTY,
            child,
            randomLiteral(DataType.TEXT),
            randomLiteral(DataType.TEXT),
            referenceAttribute(randomIdentifier(), DataType.TEXT)
        );
    }

    private static EsRelation relation() {
        return relation(List.of());
    }

    private static EsRelation relation(List<Attribute> fieldAttributes) {
        return new EsRelation(EMPTY, randomIdentifier(), randomFrom(IndexMode.values()), Map.of(), fieldAttributes);
    }
}
