/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.approximate.Random;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.alias;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.assertEqualsIgnoringIds;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.findFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.mul;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.hasSize;

public class MergeEvalAliasesTests extends AbstractLogicalPlanOptimizerTests {

    public MergeEvalAliasesTests(VersionMode versionMode) {
        super(versionMode);
    }

    /**
     * The aliases are merged into the single expression that the aggregation groups by:
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[result{r}#10],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS c#12, result{r}#10]]
     *   \_Eval[[(salary{f}#20 + emp_no{f}#14) * languages{f}#17 AS result#10]]
     *     \_EsRelation[test][_meta_field{f}#23, emp_no{f}#14, first_name{f}#18, ..]
     * }
     */
    public void testChainOfAliasesIsMergedIntoOneField() {
        var plan = plan("""
            FROM test
            | EVAL sum = salary + emp_no
            | EVAL result = sum * languages
            | STATS c = COUNT(*) BY result
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        var eval = as(aggregate.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));

        var result = as(eval.fields().get(0).child(), Mul.class);
        var sum = as(result.left(), Add.class);
        assertEquals(findFieldAttribute(plan, "salary"), sum.left());
        assertEquals(findFieldAttribute(plan, "emp_no"), sum.right());
        assertEquals(findFieldAttribute(plan, "languages"), result.right());
    }

    public void testLongerChainIsMergedIntoOneField() {
        var plan = plan("""
            FROM test
            | EVAL sum = salary + emp_no, product = sum * languages, result = product - 1
            | STATS c = COUNT(*) BY result
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        var eval = as(aggregate.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));

        var result = as(eval.fields().get(0).child(), Sub.class);
        var product = as(result.left(), Mul.class);
        as(product.left(), Add.class);
    }

    /**
     * {@code sum} is used by two fields: merging it would evaluate {@code salary + emp_no} twice.
     */
    public void testAliasUsedTwiceIsNotMerged() {
        var plan = plan("""
            FROM test
            | EVAL sum = salary + emp_no, a = sum * 2, b = sum * 3
            | KEEP a, b
            """);

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(3));

        var sum = eval.fields().get(0);
        assertEquals(sum.toAttribute(), as(eval.fields().get(1).child(), Mul.class).left());
        assertEquals(sum.toAttribute(), as(eval.fields().get(2).child(), Mul.class).left());
    }

    /**
     * {@code sum} is part of the output, so it is evaluated anyway: merging it would evaluate {@code salary + emp_no} twice.
     */
    public void testAliasUsedOutsideTheEvalIsNotMerged() {
        var plan = plan("""
            FROM test
            | EVAL sum = salary + emp_no, result = sum * languages
            | KEEP sum, result
            """);

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));
        assertEquals(eval.fields().get(0).toAttribute(), as(eval.fields().get(1).child(), Mul.class).left());
    }

    /**
     * A merged alias that is still used by another node is kept, and does not stop the aliases below it from being merged.
     */
    public void testPartialMerge() {
        var plan = plan("""
            FROM test
            | EVAL sum = salary + emp_no, product = sum * languages, result = product - 1
            | KEEP product, result
            """);

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        // sum is merged into product and pruned; product is part of the output, so it is not merged into result
        assertThat(eval.fields(), hasSize(2));
        as(as(eval.fields().get(0).child(), Mul.class).left(), Add.class);
        assertEquals(eval.fields().get(0).toAttribute(), as(eval.fields().get(1).child(), Sub.class).left());
    }

    public void testNonDeterministicAliasIsNotMerged() {
        FieldAttribute x = fieldAttribute("x", DataType.INTEGER);
        Alias random = alias("random", new Random(EMPTY, of(10)));
        Alias result = alias("result", mul(random.toAttribute(), of(2)));
        LogicalPlan plan = new Project(EMPTY, new Eval(EMPTY, relation(), List.of(random, result)), List.of(result.toAttribute()));

        assertEquals(plan, new MergeEvalAliases().apply(plan));

        // the same shape with a deterministic expression is merged, and the merged alias is removed
        Alias add = alias("add", new Add(EMPTY, x, of(1), TEST_CFG));
        Alias mulAdd = alias("mul", mul(add.toAttribute(), of(2)));
        plan = new Project(EMPTY, new Eval(EMPTY, relation(), List.of(add, mulAdd)), List.of(mulAdd.toAttribute()));

        var merged = as(new MergeEvalAliases().apply(plan), Project.class);
        var eval = as(merged.child(), Eval.class);
        List<Expression> expected = List.of(alias("mul", mul(new Add(EMPTY, x, of(1), TEST_CFG), of(2))));
        assertEqualsIgnoringIds(expected, eval.fields());
    }

    public void testIdempotent() {
        var plan = plan("""
            FROM test
            | EVAL sum = salary + emp_no, product = sum * languages
            | KEEP sum, product
            """);

        assertEquals(plan, new MergeEvalAliases().apply(plan));
    }
}
