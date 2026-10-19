/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteSurrogateExpressions;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;

/** Exercises label binding through the existing scalar substitution path without introducing an evaluator or transport type. */
public class LabelEvalTests extends AbstractNodeTestCase<LabelEval, Expression> {
    /** Factory-created metadata reads let generic node tests embed this scalar without exposing its private constructor. */
    public static LabelEval randomLabelEval() {
        Source source = SourceTests.randomSource();
        var metadata = new ReferenceAttribute(source, randomAlphaOfLength(5), DataType.TIMESERIES_DATA_TYPE);
        return (LabelEval) LabelEval.create(source, metadata, randomAlphaOfLength(5), randomLabelType(), randomConfiguration());
    }

    private static DataType randomLabelType() {
        return randomFrom(
            DataType.KEYWORD,
            DataType.INTEGER,
            DataType.LONG,
            DataType.UNSIGNED_LONG,
            DataType.DOUBLE,
            DataType.BOOLEAN,
            DataType.IP
        );
    }

    @Override
    protected LabelEval randomInstance() {
        return randomLabelEval();
    }

    @Override
    protected LabelEval mutate(LabelEval instance) {
        return (LabelEval) instance.replaceChildren(
            List.of(
                new ReferenceAttribute(instance.source(), randomAlphaOfLength(5), DataType.TIMESERIES_DATA_TYPE),
                instance.children().get(1)
            )
        );
    }

    @Override
    protected LabelEval copy(LabelEval instance) {
        return (LabelEval) instance.replaceChildren(instance.children());
    }

    /** The result type and configuration remain explicit node properties while input and label remain expression children. */
    @Override
    public void testTransform() {
        LabelEval original = randomInstance();
        String label = ((BytesRef) original.children().get(1).fold(FoldContext.small())).utf8ToString();
        assertEquals(
            List.of(original.children().getFirst(), original.children().get(1), original.dataType(), original.configuration()),
            original.nodeProperties()
        );
        DataType type = randomValueOtherThan(original.dataType(), LabelEvalTests::randomLabelType);
        assertEquals(
            LabelEval.create(original.source(), original.children().getFirst(), label, type, original.configuration()),
            original.transformPropertiesOnly(DataType.class, ignored -> type)
        );
        Configuration configuration = randomValueOtherThan(original.configuration(), () -> randomConfiguration());
        assertEquals(
            LabelEval.create(original.source(), original.children().getFirst(), label, original.dataType(), configuration),
            original.transformPropertiesOnly(Configuration.class, ignored -> configuration)
        );
    }

    /** Rewriting either expression child must preserve the remaining input, declared result type, and configuration. */
    @Override
    public void testReplaceChildren() {
        LabelEval original = randomInstance();
        var metadata = new ReferenceAttribute(original.source(), "replacement", DataType.TIMESERIES_DATA_TYPE);
        String label = randomAlphaOfLength(6);
        assertEquals(
            LabelEval.create(original.source(), metadata, label, original.dataType(), original.configuration()),
            original.replaceChildren(List.of(metadata, Literal.keyword(original.source(), label)))
        );
        Expression rewritten = original.transformUp(Literal.class, value -> Literal.keyword(value.source(), label));
        assertEquals(
            LabelEval.create(original.source(), original.children().getFirst(), label, original.dataType(), original.configuration()),
            rewritten
        );
    }

    public void testNamedLabelsAndTypedNullsAreUnchanged() {
        for (DataType type : List.of(DataType.KEYWORD, DataType.LONG, DataType.BOOLEAN, DataType.IP)) {
            Expression column = new ReferenceAttribute(Source.EMPTY, "label", type);
            assertSame(column, LabelEval.create(Source.EMPTY, column, "label", type, TEST_CFG));
            Expression missing = new Literal(Source.EMPTY, null, type);
            assertSame(missing, LabelEval.create(Source.EMPTY, missing, "label", type, TEST_CFG));
        }
        var error = expectThrows(
            IllegalArgumentException.class,
            () -> LabelEval.create(Source.EMPTY, Literal.keyword(Source.EMPTY, "value"), "label", DataType.LONG, TEST_CFG)
        );
        assertEquals("Conflicting types for label [label]", error.getMessage());
    }

    public void testMetadataReadsLowerInsideEval() {
        var metadata = new ReferenceAttribute(Source.EMPTY, "metadata", DataType.TIMESERIES_DATA_TYPE);
        for (DataType type : List.of(
            DataType.KEYWORD,
            DataType.LONG,
            DataType.INTEGER,
            DataType.UNSIGNED_LONG,
            DataType.BOOLEAN,
            DataType.IP
        )) {
            Expression expression = LabelEval.create(Source.EMPTY, metadata, "label", type, TEST_CFG);
            assertTrue(expression.resolved());
            var plan = new Eval(
                Source.EMPTY,
                new LocalRelation(Source.EMPTY, List.of(metadata), EmptyLocalSupplier.EMPTY),
                List.of(new Alias(Source.EMPTY, "bound_label", expression))
            );
            Eval lowered = (Eval) plan.transformExpressionsUp(Expression.class, SubstituteSurrogateExpressions::rule);
            Expression value = lowered.fields().getFirst().child();
            assertFalse(value.anyMatch(e -> e instanceof LabelEval));
            assertEquals(type, value.dataType());
            assertTrue(value.resolved());
            var get = asInstanceOf(TimeSeriesGet.class, type == DataType.KEYWORD ? value : value.children().getFirst());
            assertSame(metadata, get.metadata());
            assertEquals(Literal.keyword(Source.EMPTY, "label"), get.children().get(1));
        }
    }

    public void testFoldUsesTheSameReadAndConversion() {
        var metadata = new Literal(Source.EMPTY, new BytesRef("{\"sequence\":\"42\"}"), DataType.TIMESERIES_DATA_TYPE);
        Expression sequence = LabelEval.create(Source.EMPTY, metadata, "sequence", DataType.LONG, TEST_CFG);
        assertTrue(sequence.foldable());
        assertEquals(42L, sequence.fold(FoldContext.small()));
        assertNull(LabelEval.create(Source.EMPTY, metadata, "missing", DataType.KEYWORD, TEST_CFG).fold(FoldContext.small()));
    }

    public void testRewritesPreserveLabelAndType() {
        var first = new ReferenceAttribute(Source.EMPTY, "first", DataType.TIMESERIES_DATA_TYPE);
        var second = new ReferenceAttribute(Source.EMPTY, "second", DataType.TIMESERIES_DATA_TYPE);
        Expression original = LabelEval.create(Source.EMPTY, first, "label", DataType.LONG, TEST_CFG);
        Expression replaced = original.replaceChildren(List.of(second, original.children().get(1)));
        Expression expected = LabelEval.create(Source.EMPTY, second, "label", DataType.LONG, TEST_CFG);
        assertEquals(expected, replaced);
        assertEquals(expected.hashCode(), replaced.hashCode());
        assertNotEquals(original, replaced);
        assertNotEquals(replaced, LabelEval.create(Source.EMPTY, second, "other", DataType.LONG, TEST_CFG));
        assertNotEquals(replaced, LabelEval.create(Source.EMPTY, second, "label", DataType.KEYWORD, TEST_CFG));
        assertFalse(original.replaceChildren(List.of(Literal.keyword(Source.EMPTY, "invalid"), original.children().get(1))).resolved());
    }

    public void testOnlyMetadataWithSupportedLabelTypesIsWrapped() {
        var metadata = new ReferenceAttribute(Source.EMPTY, "metadata", DataType.TIMESERIES_DATA_TYPE);
        expectThrows(IllegalArgumentException.class, () -> LabelEval.create(Source.EMPTY, metadata, "", DataType.KEYWORD, TEST_CFG));
        expectThrows(
            IllegalArgumentException.class,
            () -> LabelEval.create(Source.EMPTY, metadata, "label", DataType.TIMESERIES_DATA_TYPE, TEST_CFG)
        );
    }

    public void testMustLowerBeforeEvaluationOrSerialization() throws IOException {
        var metadata = new ReferenceAttribute(Source.EMPTY, "metadata", DataType.TIMESERIES_DATA_TYPE);
        var expression = asInstanceOf(LabelEval.class, LabelEval.create(Source.EMPTY, metadata, "label", DataType.KEYWORD, TEST_CFG));
        expectThrows(UnsupportedOperationException.class, () -> expression.toEvaluator(null));
        expectThrows(UnsupportedOperationException.class, expression::getWriteableName);
        try (var out = new BytesStreamOutput()) {
            expectThrows(UnsupportedOperationException.class, () -> expression.writeTo(out));
        }
    }
}
