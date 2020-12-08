/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.optimizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
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

import java.util.Map;

public class OptimizerRunTests extends ESTestCase {

    private final SqlParser parser;
    private final IndexResolution getIndexResult;
    private final FunctionRegistry functionRegistry;
    private final Analyzer analyzer;
    private final Optimizer optimizer;

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

    public void testReducedBinaryComparisonGreaterThenOrEqual() {
        // i >= 12
        doTestBinaryComparisonReduction("((int + 1) / 2 - 3) * 4 >= 14", GreaterThanOrEqual.class, 12d);
    }

    public void testReducedBinaryComYparisonLessThen() {
        // i < -5/6
        doTestBinaryComparisonReduction("12 * (-int / 5) > (8 + 12) / 10", LessThan.class, -5d / 6);
    }

    public void testReducedBinaryComYparisonNotEquals() {
        // i != 7000
        doTestBinaryComparisonReduction("-3600 != (int - 200) / 2", NotEquals.class, -7000);
    }

    public void testReducedBinaryComparisonEquals() {
        // i = -12
        doTestBinaryComparisonReduction("2 * 3 / (4 / -int) = 18", Equals.class, -12d);
    }

    public void testReducedBinaryComparisonWithConjunction() {
        doTestBinaryComparisonReduction("2 * 3 / (4 / -int) = 18 AND int >= -12", Equals.class, -12d);
    }

    public void testReducedBinaryComparisonWithDisjunction() {
        doTestBinaryComparisonReduction("2 * 3 / (4 / -int) = 18 OR int > -12", GreaterThanOrEqual.class, -12d);
    }

    private void doTestBinaryComparisonReduction(String expression, Class<? extends BinaryComparison> binaryComparisonClass,
                                                 Number bound) {
        LogicalPlan plan = plan("SELECT some.string FROM test WHERE " + expression);

        assertTrue(plan instanceof UnaryPlan);
        UnaryPlan unaryPlan = (UnaryPlan) plan;
        assertTrue(unaryPlan.child() instanceof Filter);
        Filter filter = (Filter) unaryPlan.child();
        assertEquals(binaryComparisonClass, filter.condition().getClass());
        BinaryComparison bc = (BinaryComparison) filter.condition();

        assertTrue(bc.left() instanceof FieldAttribute);
        FieldAttribute attribute = (FieldAttribute) bc.left();
        assertEquals("int", attribute.name());

        assertTrue(bc.right() instanceof Literal);
        Literal literal = (Literal) bc.right();
        if (bound instanceof Double) {
            assertTrue(literal.value() instanceof Number);
            assertEquals(bound.doubleValue(), ((Number) literal.value()).doubleValue(), 1E-15);
        } else {
            assertEquals(bound, literal.value());
        }
    }
}
