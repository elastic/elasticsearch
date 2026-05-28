/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.mapper.FieldArrayContext.getOffsetsFieldName;
import static org.elasticsearch.index.mapper.FieldArrayContext.parseOffsetArray;

public class FieldArrayContextTests extends ESTestCase {

    public void testOffsets() throws IOException {
        var context = new FieldArrayContext();
        context.recordOffset("field", "a");
        context.recordOffset("field", "a");
        context.recordOffset("field", "b");
        context.recordOffset("field", "z");
        context.recordOffset("field", "a");
        context.recordOffset("field", "b");

        var parserContext = new TestDocumentParserContext();
        context.addToLuceneDocument(parserContext);

        var binaryDocValues = parserContext.doc().getField("field");
        int[] offsetToOrd = parseOffsetArray(new ByteArrayStreamInput(binaryDocValues.binaryValue().bytes));
        assertArrayEquals(new int[] { 0, 0, 1, 2, 0, 1 }, offsetToOrd);
    }

    public void testOffsetsWithNull() throws IOException {
        var context = new FieldArrayContext();
        context.recordNull("field");
        context.recordOffset("field", "a");
        context.recordOffset("field", "b");
        context.recordOffset("field", "z");
        context.recordNull("field");
        context.recordOffset("field", "b");

        var parserContext = new TestDocumentParserContext();
        context.addToLuceneDocument(parserContext);

        var binaryDocValues = parserContext.doc().getField("field");
        int[] offsetToOrd = parseOffsetArray(new ByteArrayStreamInput(binaryDocValues.binaryValue().bytes));
        assertArrayEquals(new int[] { -1, 0, 1, 2, -1, 1 }, offsetToOrd);
    }

    public void testEmptyOffset() throws IOException {
        var context = new FieldArrayContext();
        context.maybeRecordEmptyArray("field");

        var parserContext = new TestDocumentParserContext();
        context.addToLuceneDocument(parserContext);

        var binaryDocValues = parserContext.doc().getField("field");
        int[] offsetToOrd = parseOffsetArray(new ByteArrayStreamInput(binaryDocValues.binaryValue().bytes));
        assertArrayEquals(new int[] {}, offsetToOrd);
    }

    /**
     * Single non-null value in non-columnar mode must still emit an offsets DV so {@code synthetic_source_keep=arrays} can reconstruct
     * {@code ["a"]} rather than the bare scalar {@code "a"}. The state machine inside {@link FieldArrayContext} keeps the value in the
     * inline {@code pendingValue} slot during recording but materializes it at flush time.
     */
    public void testSingleValueEmitsOffsetsInNonColumnar() throws IOException {
        var context = new FieldArrayContext();
        context.recordOffset("field", "a");

        var parserContext = new TestDocumentParserContext();
        context.addToLuceneDocument(parserContext);

        var binaryDocValues = parserContext.doc().getField("field");
        int[] offsetToOrd = parseOffsetArray(new ByteArrayStreamInput(binaryDocValues.binaryValue().bytes));
        assertArrayEquals(new int[] { 0 }, offsetToOrd);
    }

    /**
     * Single non-null value in strict columnar must NOT emit an offsets DV — the sorted-set already carries the value and the
     * reconstructed source is rebuilt as a scalar. This is the allocation-saving path: the state machine never allocates the
     * eager {@code Offsets} structure for clean single-value-per-field documents.
     */
    public void testSingleValueSkipsOffsetEmissionInStrictColumnar() throws IOException {
        assumeTrue("strict columnar requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        var context = new FieldArrayContext();
        context.recordOffset("field", "a");

        var parserContext = strictColumnarParserContext();
        context.addToLuceneDocument(parserContext);

        assertNull(parserContext.doc().getField("field"));
    }

    /**
     * Promotion path triggered by a second scalar value: the inline {@code pendingValue} ("a") gets backfilled at offset 0, then "b"
     * lands at offset 1, etc. The encoded offsets must preserve insertion order.
     */
    public void testPromotionFromOneScalarPreservesInsertionOrder() throws IOException {
        var context = new FieldArrayContext();
        context.recordOffset("field", "a");
        context.recordOffset("field", "b");
        context.recordOffset("field", "c");

        var parserContext = new TestDocumentParserContext();
        context.addToLuceneDocument(parserContext);

        var binaryDocValues = parserContext.doc().getField("field");
        int[] offsetToOrd = parseOffsetArray(new ByteArrayStreamInput(binaryDocValues.binaryValue().bytes));
        // TreeMap iter order is a,b,c → ords 0,1,2. Slots 0,1,2 hold a,b,c in insertion order.
        assertArrayEquals(new int[] { 0, 1, 2 }, offsetToOrd);
    }

    /**
     * Promotion path where the first value isn't the lexicographically smallest: backfilling the inline slot must place "b" at offset 0
     * even though its ord (1) is greater than "a"'s ord (0).
     */
    public void testPromotionFromOneScalarWithLaterSmallerValue() throws IOException {
        var context = new FieldArrayContext();
        context.recordOffset("field", "b");
        context.recordOffset("field", "c");
        context.recordOffset("field", "a");

        var parserContext = new TestDocumentParserContext();
        context.addToLuceneDocument(parserContext);

        var binaryDocValues = parserContext.doc().getField("field");
        int[] offsetToOrd = parseOffsetArray(new ByteArrayStreamInput(binaryDocValues.binaryValue().bytes));
        // TreeMap order: a(0), b(1), c(2). Slots 0,1,2 hold b,c,a → ords 1,2,0.
        assertArrayEquals(new int[] { 1, 2, 0 }, offsetToOrd);
    }

    /**
     * A null appearing after a single scalar forces promotion: the inline "a" is backfilled at offset 0 and the null lands at offset 1.
     */
    public void testRecordNullPromotesInlineScalar() throws IOException {
        var context = new FieldArrayContext();
        context.recordOffset("field", "a");
        context.recordNull("field");

        var parserContext = new TestDocumentParserContext();
        context.addToLuceneDocument(parserContext);

        var binaryDocValues = parserContext.doc().getField("field");
        int[] offsetToOrd = parseOffsetArray(new ByteArrayStreamInput(binaryDocValues.binaryValue().bytes));
        assertArrayEquals(new int[] { 0, -1 }, offsetToOrd);
    }

    /**
     * Empty array marker on a previously-untouched field in strict columnar must not emit a DV.
     */
    public void testEmptyArrayInStrictColumnarSkipsEmission() throws IOException {
        assumeTrue("strict columnar requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        var context = new FieldArrayContext();
        context.maybeRecordEmptyArray("field");

        var parserContext = strictColumnarParserContext();
        context.addToLuceneDocument(parserContext);

        assertNull(parserContext.doc().getField("field"));
    }

    private static TestDocumentParserContext strictColumnarParserContext() {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        return new TestDocumentParserContext(settings);
    }

    public void testGetOffsetsFieldNameColumnarBranchFiresWhenAllConditionsHold() {
        FieldMapper.Builder builder = newTestBuilder("field");
        MapperBuilderContext context = MapperBuilderContext.root(true, false);
        String name = getOffsetsFieldName(
            context,
            Mapper.SourceKeepMode.NONE,
            true,
            false,
            builder,
            IndexVersion.current(),
            IndexVersions.MINIMUM_COMPATIBLE,
            true,
            true
        );
        assertEquals("field" + FieldArrayContext.OFFSETS_FIELD_NAME_SUFFIX, name);
    }

    public void testGetOffsetsFieldNameColumnarBranchSkipsWhenAnyConditionMissing() {
        FieldMapper.Builder builder = newTestBuilder("field");
        MapperBuilderContext syntheticRoot = MapperBuilderContext.root(true, false);
        MapperBuilderContext storedRoot = MapperBuilderContext.root(false, false);

        // not synthetic source
        assertNull(
            getOffsetsFieldName(
                storedRoot,
                Mapper.SourceKeepMode.NONE,
                true,
                false,
                builder,
                IndexVersion.current(),
                IndexVersions.MINIMUM_COMPATIBLE,
                true,
                true
            )
        );
        // not columnar
        assertNull(
            getOffsetsFieldName(
                syntheticRoot,
                Mapper.SourceKeepMode.NONE,
                true,
                false,
                builder,
                IndexVersion.current(),
                IndexVersions.MINIMUM_COMPATIBLE,
                false,
                true
            )
        );
        // multi_value=false
        assertNull(
            getOffsetsFieldName(
                syntheticRoot,
                Mapper.SourceKeepMode.NONE,
                true,
                false,
                builder,
                IndexVersion.current(),
                IndexVersions.MINIMUM_COMPATIBLE,
                true,
                false
            )
        );
    }

    public void testGetOffsetsFieldNameColumnarBranchExcludesCopyToAndMultiFields() {
        MapperBuilderContext context = MapperBuilderContext.root(true, false);

        // copy_to blocks the columnar branch — the target's reconstructed source would mix copy_to values with direct writes
        FieldMapper.Builder withCopyTo = newTestBuilder("field");
        withCopyTo.copyTo = FieldMapper.CopyTo.empty().withAddedFields(List.of("target"));
        assertNull(
            getOffsetsFieldName(
                context,
                Mapper.SourceKeepMode.NONE,
                true,
                false,
                withCopyTo,
                IndexVersion.current(),
                IndexVersions.MINIMUM_COMPATIBLE,
                true,
                true
            )
        );

        // multi-fields parents are blocked — the offsets sidecar on the parent would be wasted; sub-fields get their own offsets
        FieldMapper.Builder withMultiFields = newTestBuilder("field");
        withMultiFields.multiFieldsBuilder.add(newTestBuilder("sub"));
        assertNull(
            getOffsetsFieldName(
                context,
                Mapper.SourceKeepMode.NONE,
                true,
                false,
                withMultiFields,
                IndexVersion.current(),
                IndexVersions.MINIMUM_COMPATIBLE,
                true,
                true
            )
        );
    }

    public void testGetOffsetsFieldNameLegacyBranchUnchanged() {
        FieldMapper.Builder builder = newTestBuilder("field");
        MapperBuilderContext context = MapperBuilderContext.root(true, false);
        // legacy branch requires source_keep_mode=ARRAYS; the new isColumnar/multiValue flags default to false in the legacy overload
        String name = getOffsetsFieldName(
            context,
            Mapper.SourceKeepMode.ARRAYS,
            true,
            false,
            builder,
            IndexVersion.current(),
            IndexVersions.MINIMUM_COMPATIBLE
        );
        assertEquals("field" + FieldArrayContext.OFFSETS_FIELD_NAME_SUFFIX, name);
    }

    private static FieldMapper.Builder newTestBuilder(String name) {
        return new BooleanFieldMapper.Builder(name, ScriptCompiler.NONE, defaultIndexSettings());
    }
}
