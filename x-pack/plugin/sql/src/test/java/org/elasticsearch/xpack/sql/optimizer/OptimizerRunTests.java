/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.optimizer;

import org.elasticsearch.core.Tuple;
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
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.LiteralsOnTheRight;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.type.EsField;
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
import static org.elasticsearch.xpack.sql.SqlTestUtils.TEST_CFG;

public class OptimizerRunTests extends ESTestCase {

    private final SqlParser parser;
    private final IndexResolution getIndexResult;
    private final FunctionRegistry functionRegistry;
    private final Analyzer analyzer;
    private final Optimizer optimizer;
    private static final Map<String, Class<? extends BinaryComparison>> COMPARISONS = new HashMap<>() {
        {
            put(EQ.symbol(), Equals.class);
            put(NULLEQ.symbol(), NullEquals.class);
            put(NEQ.symbol(), NotEquals.class);
            put(GT.symbol(), GreaterThan.class);
            put(GTE.symbol(), GreaterThanOrEqual.class);
            put(LT.symbol(), LessThan.class);
            put(LTE.symbol(), LessThanOrEqual.class);
        }
    };
    private static final LiteralsOnTheRight LITERALS_ON_THE_RIGHT = new LiteralsOnTheRight();

    public OptimizerRunTests() {
        parser = new SqlParser();
        functionRegistry = new FunctionRegistry();

        Map<String, EsField> mapping = SqlTypesTests.loadMapping("mapping-multi-field-variation.json");

        EsIndex test = new EsIndex("test", mapping);
        getIndexResult = IndexResolution.valid(test);
        analyzer = new Analyzer(TEST_CFG, functionRegistry, getIndexResult,
            new Verifier(new Metrics(), TEST_CFG.version()));
        optimizer = new Optimizer();
    }

    private LogicalPlan plan(String sql) {
        return optimizer.optimize(analyzer.analyze(parser.createStatement(sql)));
    }

    public void testWhereClause() {
        LogicalPlan p = plan("SELECT some.string l FROM test WHERE int IS NOT NULL AND int < 10005 ORDER BY int");
        assertNotNull(p);
    }

    public void testSimplifyComparisonArithmeticCommutativeVsNonCommutativeOps() {
        doTestSimplifyComparisonArithmetics("int + 2 > 3", "int", ">", 1);
        doTestSimplifyComparisonArithmetics("2 + int > 3", "int", ">", 1);
        doTestSimplifyComparisonArithmetics("int - 2 > 3", "int", ">", 5);
        doTestSimplifyComparisonArithmetics("2 - int > 3", "int", "<", -1);
        doTestSimplifyComparisonArithmetics("int * 2 > 4", "int", ">", 2);
        doTestSimplifyComparisonArithmetics("2 * int > 4", "int", ">", 2);
        doTestSimplifyComparisonArithmetics("float / 2 > 4", "float", ">", 8d);
        doTestSimplifyComparisonArithmetics("2 / float < 4", "float", ">", .5);
    }

    public void testSimplifyComparisonArithmeticWithMultipleOps() {
        // i >= 3
        doTestSimplifyComparisonArithmetics("((int + 1) * 2 - 4) * 4 >= 16", "int", ">=", 3);
    }

    public void testSimplifyComparisonArithmeticWithFieldNegation() {
        doTestSimplifyComparisonArithmetics("12 * (-int - 5) >= -120", "int", "<=", 5);
    }

    public void testSimplifyComparisonArithmeticWithFieldDoubleNegation() {
        doTestSimplifyComparisonArithmetics("12 * -(-int - 5) <= 120", "int", "<=", 5);
    }

    public void testSimplifyComparisonArithmeticWithConjunction() {
        doTestSimplifyComparisonArithmetics("12 * (-int - 5) = -120 AND int < 6 ", "int", "==", 5);
    }

    public void testSimplifyComparisonArithmeticWithDisjunction() {
        doTestSimplifyComparisonArithmetics("12 * (-int - 5) >= -120 OR int < 5", "int", "<=", 5);
    }

    public void testSimplifyComparisonArithmeticWithFloatsAndDirectionChange() {
        doTestSimplifyComparisonArithmetics("float / -2 < 4", "float", ">", -8d);
        doTestSimplifyComparisonArithmetics("float * -2 < 4", "float", ">", -2d);
    }

    public void testSimplyComparisonArithmeticWithUnfoldedProd() {
        assertSemanticMatching("int * int >= 3", "((int * int + 1) * 2 - 4) * 4 >= 16");
    }

    public void testSimplifyComparisonArithmeticSkippedOnIntegerArithmeticalOverflow() {
        assertNotSimplified("int - 1 " + randomBinaryComparison() + " " + Long.MAX_VALUE);
        assertNotSimplified("1 - int " + randomBinaryComparison() + " " + Long.MIN_VALUE);
        assertNotSimplified("int - 1 " + randomBinaryComparison() + " " + Integer.MAX_VALUE);
        assertNotSimplified("1 - int " + randomBinaryComparison() + " " + Integer.MIN_VALUE);
    }

    public void testSimplifyComparisonArithmeticSkippedOnIntegerArithmeticalOverflowOnNegation() {
        assertNotSimplified("-int " + randomBinaryComparison() + " " + Long.MIN_VALUE);
        assertNotSimplified("-int " + randomBinaryComparison() + " " + Integer.MIN_VALUE);
    }

    public void testSimplifyComparisonArithmeticSkippedOnFloatingPointArithmeticalOverflow() {
        assertNotSimplified("float / 10 " + randomBinaryComparison() + " " + Float.MAX_VALUE);
        assertNotSimplified("float / " + Float.MAX_VALUE +" " + randomBinaryComparison() + " 10");
        assertNotSimplified("float / 10 " + randomBinaryComparison() + " " + Double.MAX_VALUE);
        assertNotSimplified("float / " + Double.MAX_VALUE + " " + randomBinaryComparison() + " 10");
        // note: the "reversed" test (i.e.: MAX_VALUE / float < literal) would require a floating literal, which is skipped for other
        // reason (see testSimplifyComparisonArithmeticSkippedOnFloats())
    }

    public void testSimplifyComparisonArithmeticSkippedOnNegatingOverflow() {
        assertNotSimplified("-int " + randomBinaryComparison() + " " + Long.MIN_VALUE);
        assertNotSimplified("-int " + randomBinaryComparison() + " " + Integer.MIN_VALUE);
    }

    public void testSimplifyComparisonArithmeticSkippedOnDateOverflow() {
        assertNotSimplified("date - INTERVAL 999999999 YEAR > '2010-01-01T01:01:01'::DATETIME");
        assertNotSimplified("date + INTERVAL -999999999 YEAR > '2010-01-01T01:01:01'::DATETIME");
    }

    public void testSimplifyComparisonArithmeticSkippedOnMulDivByZero() {
        assertNotSimplified("float / 0 " + randomBinaryComparison() + " 1");
        assertNotSimplified("float * 0 " + randomBinaryComparison() + " 1");
        assertNotSimplified("int / 0 " + randomBinaryComparison() + " 1");
        assertNotSimplified("int * 0 " + randomBinaryComparison() + " 1");
    }

    public void testSimplifyComparisonArithmeticSkippedOnDiv() {
        assertNotSimplified("int / 4 " + randomBinaryComparison() + " 1");
        assertNotSimplified("4 / int " + randomBinaryComparison() + " 1");
    }

    public void testSimplifyComparisonArithmeticSkippedOnResultingFloatLiteral() {
        assertNotSimplified("int * 2 " + randomBinaryComparison() + " 3");
    }

    public void testSimplifyComparisonArithmeticSkippedOnFloatFieldWithPlusMinus() {
        assertNotSimplified("float + 4 " + randomBinaryComparison() + " 1");
        assertNotSimplified("4 + float " + randomBinaryComparison() + " 1");
        assertNotSimplified("float - 4 " + randomBinaryComparison() + " 1");
        assertNotSimplified("4 - float " + randomBinaryComparison() + " 1");
    }

    public void testSimplifyComparisonArithmeticSkippedOnFloats() {
        for (String field : List.of("int", "float")) {
            for (Tuple<? extends Number, ? extends Number> nr : List.of(new Tuple<>(.4, 1), new Tuple<>(1, .4))) {
                assertNotSimplified(field + " + " + nr.v1() + " " + randomBinaryComparison() + " " + nr.v2());
                assertNotSimplified(field + " - " + nr.v1() + " " + randomBinaryComparison() + " " + nr.v2());
                assertNotSimplified(nr.v1()+ " + " + field  + " " + randomBinaryComparison() + " " + nr.v2());
                assertNotSimplified(nr.v1()+ " - " + field  + " " + randomBinaryComparison() + " " + nr.v2());
            }
        }
    }

    public void testSimplifyComparisonArithmeticWithDateTime() {
        doTestSimplifyComparisonArithmetics("date - INTERVAL 1 MONTH > '2010-01-01T01:01:01'::DATETIME", "date", ">",
            ZonedDateTime.parse("2010-02-01T01:01:01Z"));
    }

    public void testSimplifyComparisonArithmeticWithDate() {
        doTestSimplifyComparisonArithmetics("date + INTERVAL 1 YEAR <= '2011-01-01T00:00:00'::DATE", "date", "<=",
            ZonedDateTime.parse("2010-01-01T00:00:00Z"));
    }

    public void testSimplifyComparisonArithmeticWithDateAndMultiplication() {
        // the multiplication should be folded, but check
        doTestSimplifyComparisonArithmetics("date + 2 * INTERVAL 1 YEAR <= '2012-01-01T00:00:00'::DATE", "date", "<=",
            ZonedDateTime.parse("2010-01-01T00:00:00Z"));
    }

    private void doTestSimplifyComparisonArithmetics(String expression, String fieldName, String compSymbol, Object bound) {
        BinaryComparison bc = extractPlannedBinaryComparison(expression);
        assertEquals(compSymbol, bc.symbol());
        assertTrue(COMPARISONS.get(compSymbol).isInstance(bc));

        assertTrue(bc.left() instanceof FieldAttribute);
        FieldAttribute attribute = (FieldAttribute) bc.left();
        assertEquals(fieldName, attribute.name());

        assertTrue(bc.right() instanceof Literal);
        Literal literal = (Literal) bc.right();
        assertEquals(bound, literal.value());
    }

    private void assertNotSimplified(String condition) {
        assertSemanticMatching(extractPlannedBinaryComparison(condition), parser.createExpression(condition));
    }

    private void assertSemanticMatching(String expected, String provided) {
        BinaryComparison bc = extractPlannedBinaryComparison(provided);
        Expression exp = parser.createExpression(expected);
        assertSemanticMatching(bc, exp);
    }

    private BinaryComparison extractPlannedBinaryComparison(String expression) {
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
            .transformUp(LITERALS_ON_THE_RIGHT.expressionToken(), LITERALS_ON_THE_RIGHT::rule)
            .transformUp(x -> x.foldable() ? new Literal(x.source(), x.fold(), x.dataType()) : x);

        List<Expression> resolvedFields = fieldAttributeExp.collectFirstChildren(x -> x instanceof FieldAttribute);
        for (Expression field : resolvedFields) {
            FieldAttribute fa = (FieldAttribute) field;
            unresolvedUpdated = unresolvedUpdated.transformDown(UnresolvedAttribute.class, x -> x.name().equals(fa.name()) ? fa : x);
        }

        assertTrue(unresolvedUpdated.semanticEquals(fieldAttributeExp));
    }

    private static String randomBinaryComparison() {
        return randomFrom(COMPARISONS.keySet().stream().map(x -> EQ.symbol().equals(x) ? "=" : x).collect(Collectors.toSet()));
    }
}
