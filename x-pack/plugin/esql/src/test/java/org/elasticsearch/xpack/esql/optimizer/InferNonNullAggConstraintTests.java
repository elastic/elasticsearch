/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_SEARCH_STATS;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.asLimit;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

public class InferNonNullAggConstraintTests extends AbstractLocalLogicalPlanOptimizerTests {

    /**
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false]
     * \_Aggregate[[],[SUM($$integer_long_field$converted_to$long{f$}#5,true[BOOLEAN]) AS sum(integer_long_field::long)#3]]
     *   \_Filter[ISNOTNULL($$integer_long_field$converted_to$long{f$}#5)]
     *     \_EsRelation[test*][!integer_long_field, $$integer_long_field$converted..]
     * }
     */
    public void testUnionTypesInferNonNullAggConstraint() {
        LogicalPlan coordinatorOptimized = optimize(
            analyzerWithUnionTypeMapping().analyze(TEST_PARSER.parseQuery("FROM test* | STATS sum(integer_long_field::long)"))
        );
        var plan = localPlan(coordinatorOptimized, TEST_SEARCH_STATS);

        var limit = asLimit(plan, 1000);
        var agg = as(limit.child(), Aggregate.class);
        var filter = as(agg.child(), Filter.class);
        var relation = as(filter.child(), EsRelation.class);

        var isNotNull = as(filter.condition(), IsNotNull.class);
        var unionTypeField = as(isNotNull.field(), FieldAttribute.class);
        assertEquals("$$integer_long_field$converted_to$long", unionTypeField.name());
        assertEquals("integer_long_field", unionTypeField.fieldName().string());
    }

    public void testInferNonNullAggConstraint_noFieldFilters() {
        // The queries don't get an additional filter.
        for (String query : List.of(
            "FROM test_all | STATS COUNT(*)",
            "FROM test_all | STATS SUM(long), COUNT(*)",
            "FROM test_all | STATS AVG(COALESCE(long, 7))",
            "FROM test_all | MV_EXPAND long | STATS AVG(long)",
            "FROM test_all | STATS FIRST(long, long)",
            "FROM test_all | STATS AVG(long), LAST(long, long), MEDIAN(long)",
            "FROM test_all | GROK text \"blah %{EMAILADDRESS:email} blah\" | STATS AVG(LENGTH(email))",
            "FROM test_all | EVAL long = 2*long | STATS SUM(long)"  // SUM(long) can't see the original long (TODO: make smarter)
        )) {
            var plan = allTypes().localPlan(query);
            var aggregate = as(plan.collectFirstChildren(Aggregate.class::isInstance).get(0), Aggregate.class);
            var filters = new ArrayList<Filter>();
            aggregate.forEachDown(Filter.class, filters::add);
            assertThat(query, filters, empty());
        }
    }

    public void testInferNonNullAggConstraint_oneFieldFilter() {
        // These queries all get the filter: "WHERE long IS NOT NULL"
        for (String query : List.of(
            "FROM test_all | STATS AVG(long)",
            "FROM test_all | STATS SUM(42*long+7)",
            "FROM test_all | STATS AVG(long) + SUM(long*long) + MEDIAN(POW(TO_DOUBLE(long), TO_LONG(3)))",
            "FROM test_all | STATS POW(MEDIAN(long+long*2-42/long), 2)",
            "FROM test_all | STATS SUM(long + COALESCE(double, 42))",
            "FROM test_all | EVAL blah = REPEAT(\"blah\", long::integer) | STATS AVG(LENGTH(blah))",
            "FROM test_all | EVAL long2 = 2*long | STATS SUM(long2)",
            "FROM test_all | RENAME long AS x | STATS SUM(x)"
        )) {
            var plan = allTypes().localPlan(query);
            var aggregate = as(plan.collectFirstChildren(Aggregate.class::isInstance).get(0), Aggregate.class);
            var filters = new ArrayList<Filter>();
            aggregate.forEachDown(Filter.class, filters::add);
            assertThat(filters, hasSize(1));
            var isNotNull = as(filters.get(0).condition(), IsNotNull.class);
            var field = as(isNotNull.field(), FieldAttribute.class);
            assertEquals("long", field.fieldName().string());
        }
    }

    public void testInferNonNullAggConstraint_orMultipleFieldFilters() {
        // These queries all get the filter: "WHERE (long IS NOT NULL) OR (double IS NOT NULL)"
        for (String query : List.of(
            "FROM test_all | STATS AVG(double), MEDIAN(long)",
            "FROM test_all | STATS MEDIAN(SQRT(1/(double+7/double))), COUNT_DISTINCT(long)",
            "FROM test_all | EVAL blah = REPEAT(\"blah\", double::integer) | STATS AVG(LENGTH(blah)), SUM(long)"
        )) {
            var plan = allTypes().localPlan(query);
            var aggregate = as(plan.collectFirstChildren(Aggregate.class::isInstance).get(0), Aggregate.class);
            var filters = new ArrayList<Filter>();
            aggregate.forEachDown(Filter.class, filters::add);
            assertThat(filters, hasSize(1));
            var or = as(filters.get(0).condition(), Or.class);
            var left = as(or.left(), IsNotNull.class);
            var right = as(or.right(), IsNotNull.class);
            var fields = List.of(
                as(left.field(), FieldAttribute.class).fieldName().string(),
                as(right.field(), FieldAttribute.class).fieldName().string()
            );
            assertThat(query, fields, containsInAnyOrder("double", "long"));
        }
    }

    public void testInferNonNullAggConstraint_andMultipleFieldFilters() {
        // These queries all get the filter: "WHERE (long IS NOT NULL) AND (double IS NOT NULL)"
        for (String query : List.of(
            "FROM test_all | STATS AVG(double + long), MEDIAN(long * double), 2+POW(SUM(SQRT(1/double + SIN(1/1/1/1/1/1/long))),3)",
            "FROM test_all | EVAL blah = REPEAT(double::string, long::integer) | STATS AVG(LENGTH(blah))"
        )) {
            var plan = allTypes().localPlan(query);
            var aggregate = as(plan.collectFirstChildren(Aggregate.class::isInstance).get(0), Aggregate.class);
            var filters = new ArrayList<Filter>();
            aggregate.forEachDown(Filter.class, filters::add);
            assertThat(filters, hasSize(1));
            var and = as(filters.get(0).condition(), And.class);
            var left = as(and.left(), IsNotNull.class);
            var right = as(and.right(), IsNotNull.class);
            var fields = List.of(
                as(left.field(), FieldAttribute.class).fieldName().string(),
                as(right.field(), FieldAttribute.class).fieldName().string()
            );
            assertThat(query, fields, containsInAnyOrder("double", "long"));
        }
    }

    private static Analyzer analyzerWithUnionTypeMapping() {
        InvalidMappedField unionTypeField = new InvalidMappedField(
            "integer_long_field",
            Map.of("integer", Set.of("test1"), "long", Set.of("test2"))
        );

        EsIndex test = EsIndexGenerator.esIndex(
            "test*",
            Map.of("integer_long_field", unionTypeField),
            Map.of("test1", IndexMode.STANDARD, "test2", IndexMode.STANDARD)
        );

        return analyzer().addIndex(test).buildAnalyzer();
    }
}
