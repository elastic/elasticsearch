/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/** Materialization must carry usable-column declarations and unusable-column diagnostics through the same boundary. */
public class SequenceMaterializedAttributeTests extends ESTestCase {
    /** A materialized value keeps its declaration while losing the ability to use an indexed-field evaluator. */
    public void testDeclaredAnalyzerStaysOnRuntimeReference() {
        Attribute source = new ReferenceAttribute(Source.EMPTY, null, "text", DataType.TEXT, Nullability.TRUE, null, false, "whitespace");
        NameId binding = new NameId();
        Attribute output = SequenceSchema.reference(source, Source.EMPTY, "stage1.text", binding, true);
        assertThat(output, instanceOf(ReferenceAttribute.class));
        assertEquals("whitespace", AnalyzedTextExpression.valuesAnalyzerOf(output));
        assertEquals(binding, output.id());
        assertNotEquals(source.id(), output.id());
        assertEquals("whitespace", AnalyzedTextExpression.valuesAnalyzerOf(output.withName("renamed")));
    }

    /** A portable mapped analyzer is retained as a values declaration, while the source loses indexed-field identity. */
    public void testRegisteredMappedAnalyzerStaysOnRuntimeReference() {
        FieldAttribute source = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "text",
            new TextEsField(
                "text",
                Map.of(),
                false,
                false,
                EsField.TimeSeriesFieldType.UNKNOWN,
                "keyword",
                TextEsField.DEFAULT_POSITION_INCREMENT_GAP,
                TextEsField.UnknownAnalyzer.NONE,
                null
            )
        );
        Attribute output = SequenceSchema.reference(source, Source.EMPTY, "stage1.text", new NameId(), false);
        assertThat(output, instanceOf(ReferenceAttribute.class));
        assertEquals("keyword", AnalyzedTextExpression.valuesAnalyzerOf(output));
    }

    /** Unsupported values remain projectable NULLs with diagnostics, rather than ordinary usable nullable values. */
    public void testUnsupportedRestrictionAndOriginalTypesSurvive() {
        UnsupportedAttribute source = new UnsupportedAttribute(Source.EMPTY, "bad", new UnsupportedEsField("bad", List.of("completion")));
        NameId binding = new NameId();
        Attribute output = SequenceSchema.reference(source, Source.EMPTY, "stage1.bad", binding, false);
        assertThat(output, instanceOf(UnsupportedAttribute.class));
        UnsupportedAttribute unsupported = (UnsupportedAttribute) output;
        assertFalse(unsupported.resolved());
        assertEquals(List.of("completion"), unsupported.originalTypes());
        assertEquals(binding, unsupported.id());
        assertThat(unsupported.unresolvedMessage(), containsString("completion"));
        var projection = SequenceSchema.projection(output, "bad", binding, false);
        assertThat(projection, instanceOf(UnsupportedAttribute.class));
        assertEquals(binding, projection.id());
        assertEquals(List.of("completion"), ((UnsupportedAttribute) projection).originalTypes());
        var passThrough = SequenceSchema.projection(output, "bad", null, false);
        assertThat(passThrough, instanceOf(UnsupportedAttribute.class));
        assertEquals(output.id(), passThrough.id());
        assertEquals(List.of("completion"), ((UnsupportedAttribute) passThrough).originalTypes());
    }

    /** Two supported but incompatible mapped types must retain the ordinary ambiguity error across the boundary. */
    public void testMappingConflictKeepsOrdinaryDiagnosticAndOriginalTypes() {
        FieldAttribute source = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "bad",
            new InvalidMappedField("bad", Map.of("keyword", Set.of("keyword-index"), "long", Set.of("long-index")))
        );
        Attribute output = SequenceSchema.reference(source, Source.EMPTY, "stage1.bad", new NameId(), false);
        assertThat(output, instanceOf(UnsupportedAttribute.class));
        UnsupportedAttribute unsupported = (UnsupportedAttribute) output;
        assertEquals(Set.of("keyword", "long"), Set.copyOf(unsupported.originalTypes()));
        assertThat(unsupported.unresolvedMessage(), containsString("Cannot use field [bad] due to ambiguities"));
        assertThat(unsupported.unresolvedMessage(), containsString("keyword"));
        assertThat(unsupported.unresolvedMessage(), containsString("long"));
    }
}
