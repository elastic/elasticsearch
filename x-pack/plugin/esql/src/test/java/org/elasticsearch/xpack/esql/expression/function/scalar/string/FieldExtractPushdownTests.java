/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression.PushedBlockLoaderExpression;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.Collections;

/**
 * Unit tests for {@link FieldExtract#tryPushToFieldLoading} (block-loader pushdown, Phase C).
 * <p>
 *     Pushdown happens when the first argument is a {@link FieldAttribute} of type
 *     {@link DataType#FLATTENED} and the path argument folds to a literal flat sub-field name
 *     that passes {@link FieldExtract#validateFieldExtractPath(String)}. The function then fuses
 *     into the keyed sub-field doc-values loader on the data node, skipping per-row JSON
 *     materialization. The path is the literal storage key (no JSONPath), so brackets and array
 *     indices are rejected by the validator and pushdown returns {@code null} for them.
 * </p>
 * <p>
 *     The {@code FIELD_EXTRACT_PUSHDOWN} capability gate is set at JVM start (snapshot vs release
 *     build), so tests use {@code assumeTrue}/{@code assumeFalse} to run in the right build mode
 *     instead of toggling the gate at runtime.
 * </p>
 */
public class FieldExtractPushdownTests extends ESTestCase {

    private static final FieldAttribute FLATTENED_ROOT = flattenedField("resource.attributes");

    public void testPushdownReturnsExtractFlattenedSubfieldForLiteralKey() {
        assumeTrue("FIELD_EXTRACT_PUSHDOWN must be enabled for the happy path", EsqlCapabilities.Cap.FIELD_EXTRACT_PUSHDOWN.isEnabled());
        FieldExtract fn = new FieldExtract(Source.EMPTY, FLATTENED_ROOT, keywordLiteral("host.name"));

        PushedBlockLoaderExpression pushed = fn.tryPushToFieldLoading(SearchStats.EMPTY);

        assertNotNull("expected pushdown to fire for a literal flat key", pushed);
        assertSame(FLATTENED_ROOT, pushed.field());
        assertEquals(new BlockLoaderFunctionConfig.ExtractFlattenedSubfield("host.name"), pushed.config());
    }

    public void testPushdownReturnsNullWhenCapabilityDisabled() {
        // The gate is set at JVM start; this test is meaningful only when the build leaves it disabled
        // (release builds). On snapshot builds the happy-path test above already exercises the on-state.
        assumeFalse(
            "This test verifies the disabled branch. Only meaningful when FIELD_EXTRACT_PUSHDOWN is off",
            EsqlCapabilities.Cap.FIELD_EXTRACT_PUSHDOWN.isEnabled()
        );
        FieldExtract fn = new FieldExtract(Source.EMPTY, FLATTENED_ROOT, keywordLiteral("host.name"));

        assertNull(
            "with FIELD_EXTRACT_PUSHDOWN disabled the function must keep its per-row evaluator",
            fn.tryPushToFieldLoading(SearchStats.EMPTY)
        );
    }

    public void testPushdownReturnsNullWhenFieldIsNotFieldAttribute() {
        assumeTrue("FIELD_EXTRACT_PUSHDOWN must be enabled", EsqlCapabilities.Cap.FIELD_EXTRACT_PUSHDOWN.isEnabled());
        Expression nonFieldAttr = new ReferenceAttribute(Source.EMPTY, "synthetic_root", DataType.FLATTENED);
        FieldExtract fn = new FieldExtract(Source.EMPTY, nonFieldAttr, keywordLiteral("host.name"));

        assertNull(
            "pushdown must require a real FieldAttribute, not a ReferenceAttribute or other expression",
            fn.tryPushToFieldLoading(SearchStats.EMPTY)
        );
    }

    public void testPushdownReturnsNullWhenFieldTypeIsNotFlattened() {
        assumeTrue("FIELD_EXTRACT_PUSHDOWN must be enabled", EsqlCapabilities.Cap.FIELD_EXTRACT_PUSHDOWN.isEnabled());
        FieldAttribute keywordRoot = new FieldAttribute(
            Source.EMPTY,
            "host.name",
            new EsField("host.name", DataType.KEYWORD, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
        FieldExtract fn = new FieldExtract(Source.EMPTY, keywordRoot, keywordLiteral("host.name"));

        assertNull("pushdown must require FLATTENED type on the field argument", fn.tryPushToFieldLoading(SearchStats.EMPTY));
    }

    public void testPushdownReturnsNullWhenPathIsNotFoldable() {
        assumeTrue("FIELD_EXTRACT_PUSHDOWN must be enabled", EsqlCapabilities.Cap.FIELD_EXTRACT_PUSHDOWN.isEnabled());
        Expression nonFoldablePath = new ReferenceAttribute(Source.EMPTY, "path_column", DataType.KEYWORD);
        FieldExtract fn = new FieldExtract(Source.EMPTY, FLATTENED_ROOT, nonFoldablePath);

        assertNull(
            "pushdown must require a foldable (constant) path. The keyed loader can't be built per row",
            fn.tryPushToFieldLoading(SearchStats.EMPTY)
        );
    }

    public void testPushdownReturnsNullWhenPathIsBracketed() {
        assumeTrue("FIELD_EXTRACT_PUSHDOWN must be enabled", EsqlCapabilities.Cap.FIELD_EXTRACT_PUSHDOWN.isEnabled());
        // Brackets are JSONPath syntax. The verifier rejects them at type-resolution time, but
        // tryPushToFieldLoading defends in depth via validateFieldExtractPath and returns null
        // if a bracketed path somehow reaches it.
        FieldExtract fn = new FieldExtract(Source.EMPTY, FLATTENED_ROOT, keywordLiteral("['host.name']"));

        assertNull("pushdown must reject bracketed paths, those are JSONPath syntax", fn.tryPushToFieldLoading(SearchStats.EMPTY));
    }

    public void testPushdownReturnsNullWhenPathContainsArrayIndex() {
        assumeTrue("FIELD_EXTRACT_PUSHDOWN must be enabled", EsqlCapabilities.Cap.FIELD_EXTRACT_PUSHDOWN.isEnabled());
        // Array indices are JSONPath syntax and the validator rejects them too.
        FieldExtract fn = new FieldExtract(Source.EMPTY, FLATTENED_ROOT, keywordLiteral("tags[0]"));

        assertNull("pushdown must reject paths that include array indices", fn.tryPushToFieldLoading(SearchStats.EMPTY));
    }

    public void testPushdownReturnsNullWhenPathIsEmpty() {
        assumeTrue("FIELD_EXTRACT_PUSHDOWN must be enabled", EsqlCapabilities.Cap.FIELD_EXTRACT_PUSHDOWN.isEnabled());
        FieldExtract fn = new FieldExtract(Source.EMPTY, FLATTENED_ROOT, keywordLiteral(""));

        assertNull("pushdown must reject empty paths, the validator rejects them", fn.tryPushToFieldLoading(SearchStats.EMPTY));
    }

    public void testPushdownPreservesDottedKeyVerbatim() {
        assumeTrue("FIELD_EXTRACT_PUSHDOWN must be enabled", EsqlCapabilities.Cap.FIELD_EXTRACT_PUSHDOWN.isEnabled());
        // A dotted key like "service.attributes.host.name" is a single literal storage key for the
        // flattened root. The dot is part of the key, not a path separator, so pushdown forwards
        // the whole string to the keyed sub-field loader as is.
        String dottedKey = "service.attributes.host.name";
        FieldExtract fn = new FieldExtract(Source.EMPTY, FLATTENED_ROOT, keywordLiteral(dottedKey));

        PushedBlockLoaderExpression pushed = fn.tryPushToFieldLoading(SearchStats.EMPTY);

        assertNotNull(pushed);
        assertEquals(new BlockLoaderFunctionConfig.ExtractFlattenedSubfield(dottedKey), pushed.config());
    }

    private static FieldAttribute flattenedField(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.FLATTENED, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static Literal keywordLiteral(String value) {
        return new Literal(Source.EMPTY, new BytesRef(value), DataType.KEYWORD);
    }
}
