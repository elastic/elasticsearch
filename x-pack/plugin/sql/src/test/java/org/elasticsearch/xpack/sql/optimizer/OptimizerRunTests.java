/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.optimizer;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanLiteralsOnTheRight;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.types.SqlTypesTests;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation.EQ;
import static org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation.GT;
import static org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation.GTE;
import static org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation.LT;
import static org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation.LTE;
import static org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation.NEQ;
import static org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation.NULLEQ;

public class OptimizerRunTests extends ESTestCase {

    private final SqlParser parser;
    private final IndexResolution getIndexResult;
    private final FunctionRegistry functionRegistry;
    private final Analyzer analyzer;
    private final Optimizer optimizer;
    private static final BooleanLiteralsOnTheRight LITERALS_ON_THE_RIGHT = new BooleanLiteralsOnTheRight();

    public OptimizerRunTests() {
        parser = new SqlParser();
        functionRegistry = new FunctionRegistry();

        Map<String, EsField> mapping = SqlTypesTests.loadMapping("mapping-multi-field-variation.json");

        EsIndex test = new EsIndex("test", mapping);
        getIndexResult = IndexResolution.valid(test);
        analyzer = new Analyzer(SqlTestUtils.TEST_CFG, functionRegistry, getIndexResult, new Verifier(new Metrics()));
        optimizer = new Optimizer();
    }

    private LogicalPlan plan(String sql) {
        return optimizer.optimize(analyzer.analyze(parser.createStatement(sql)));
    }

    public void testWhereClause() {
        LogicalPlan p = plan("SELECT some.string l FROM test WHERE int IS NOT NULL AND int < 10005 ORDER BY int");
        assertNotNull(p);
    }

    public void testBubbleUpNegation() {
        String provided = "(1 / -int) * (-float) * (-float / (2 / -int)) * -2 * (-float - int) * (-(-int)) * (-float / (-int + 1)) > 1";
        String expected = "- ((1/int) * float * (float / (2 / int)) * -2 * (-float - int) * int * (float / (-int + 1))) > 1";
        BinaryComparison bc = extractBinaryComparison(provided);
        Expression exp = parser.createExpression(expected);
        assertSemanticMatching(bc, exp);
    }

    private BinaryComparison extractBinaryComparison(String expression) {
        LogicalPlan plan = planWithArithmeticCondition(expression);

        assertTrue(plan instanceof UnaryPlan);
        UnaryPlan unaryPlan = (UnaryPlan) plan;
        assertTrue(unaryPlan.child() instanceof Filter);
        Filter filter = (Filter) unaryPlan.child();
        assertTrue(filter.condition() instanceof BinaryComparison);
        return (BinaryComparison) filter.condition();
    }

    private LogicalPlan planWithArithmeticCondition(String condition) {
        return plan("SELECT some.string FROM test WHERE " + condition);
    }

    private static void assertSemanticMatching(Expression fieldAttributeExp, Expression unresolvedAttributeExp) {
        Expression unresolvedUpdated = unresolvedAttributeExp
            .transformUp(LITERALS_ON_THE_RIGHT::rule)
            .transformUp(x -> x.foldable() ? new Literal(x.source(), x.fold(), x.dataType()) : x);

        List<Expression> resolvedFields = fieldAttributeExp.collectFirstChildren(x -> x instanceof FieldAttribute);
        for (Expression field : resolvedFields) {
            FieldAttribute fa = (FieldAttribute) field;
            unresolvedUpdated = unresolvedUpdated.transformDown(UnresolvedAttribute.class, x -> x.name().equals(fa.name()) ? fa : x);
        }

        assertTrue(unresolvedUpdated.semanticEquals(fieldAttributeExp));
    }
}
