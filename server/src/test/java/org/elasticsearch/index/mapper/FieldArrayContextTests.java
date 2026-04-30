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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.mapper.FieldArrayContext.parseOffsetArray;
import static org.hamcrest.Matchers.containsString;

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
     * {@code multi_value=array} short-circuits the runtime safety checks because mapping-time validation enforces them. The helper must
     * return the offsets-name regardless of {@code source_keep_mode}, {@code stored}, or synthetic-source mode.
     */
    public void testGetOffsetsFieldNameMultiValueArrayShortCircuits() {
        var builder = new TestBuilder("field");
        var context = MapperBuilderContext.root(false, false);

        String name = FieldArrayContext.getOffsetsFieldName(
            context,
            Mapper.SourceKeepMode.NONE,
            randomBoolean(),
            randomBoolean(),
            builder,
            IndexVersion.current(),
            IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_KEYWORD,
            FieldMapper.DocValuesParameter.Values.MultiValue.ARRAY
        );
        assertEquals("field" + ".offsets", name);
    }

    /**
     * Non-ARRAY values fall through to the existing {@code source_keep_mode=arrays} runtime gate, which requires synthetic source.
     * Without it, the helper returns {@code null} regardless of the multi-value mode.
     */
    public void testGetOffsetsFieldNameNonArrayFallsThroughToExistingGate() {
        var builder = new TestBuilder("field");
        var context = MapperBuilderContext.root(false, false);

        for (var mv : List.of(
            FieldMapper.DocValuesParameter.Values.MultiValue.NO,
            FieldMapper.DocValuesParameter.Values.MultiValue.SORTED,
            FieldMapper.DocValuesParameter.Values.MultiValue.SORTED_SET
        )) {
            String name = FieldArrayContext.getOffsetsFieldName(
                context,
                Mapper.SourceKeepMode.ARRAYS,
                true,
                false,
                builder,
                IndexVersion.current(),
                IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_KEYWORD,
                mv
            );
            // synthetic source disabled, runtime gate denies
            assertNull("multi_value=" + mv + " must fall through to runtime gate", name);
        }
    }

    /**
     * Synthetic source plus {@code source_keep_mode=arrays} on a non-ARRAY multi-value should still arm the existing runtime gate.
     */
    public void testGetOffsetsFieldNameSourceKeepModeArraysTriggersExistingPath() {
        var builder = new TestBuilder("field");
        var context = MapperBuilderContext.root(true, false);

        String name = FieldArrayContext.getOffsetsFieldName(
            context,
            Mapper.SourceKeepMode.ARRAYS,
            true,
            false,
            builder,
            IndexVersion.current(),
            IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_KEYWORD,
            FieldMapper.DocValuesParameter.Values.MultiValue.SORTED_SET
        );
        assertEquals("field.offsets", name);
    }

    public void testValidateMultiValueArrayCopyToRejected() {
        var builder = new TestBuilder("field");
        builder.copyTo = builder.copyTo.withAddedFields(List.of("other"));
        var context = MapperBuilderContext.root(false, false);

        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> FieldArrayContext.validateMultiValueArray(
                "field",
                context,
                true,
                builder,
                IndexVersion.current(),
                IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_KEYWORD
            )
        );
        assertThat(ex.getMessage(), containsString("[multi_value=array] which is incompatible with [copy_to]"));
    }

    public void testValidateMultiValueArrayMultiFieldsRejected() {
        var builder = new TestBuilder("field");
        builder.multiFieldsBuilder.add(new TestBuilder("sub"));
        var context = MapperBuilderContext.root(false, false);

        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> FieldArrayContext.validateMultiValueArray(
                "field",
                context,
                true,
                builder,
                IndexVersion.current(),
                IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_KEYWORD
            )
        );
        assertThat(ex.getMessage(), containsString("[multi_value=array] which is incompatible with [fields]"));
    }

    public void testValidateMultiValueArrayDocValuesFalseRejected() {
        var builder = new TestBuilder("field");
        var context = MapperBuilderContext.root(false, false);

        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> FieldArrayContext.validateMultiValueArray(
                "field",
                context,
                false,
                builder,
                IndexVersion.current(),
                IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_KEYWORD
            )
        );
        assertThat(ex.getMessage(), containsString("[multi_value=array] which requires [doc_values] to be enabled"));
    }

    public void testValidateMultiValueArrayNestedRejected() {
        var builder = new TestBuilder("field");
        // Use the package-private constructor directly to flip inNestedContext=true without instantiating NestedObjectMapper.
        var context = new MapperBuilderContext(
            null,
            false,
            false,
            false,
            ObjectMapper.Defaults.DYNAMIC,
            MapperService.MergeReason.MAPPING_UPDATE,
            true
        );

        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> FieldArrayContext.validateMultiValueArray(
                "field",
                context,
                true,
                builder,
                IndexVersion.current(),
                IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_KEYWORD
            )
        );
        assertThat(ex.getMessage(), containsString("[multi_value=array] which is not supported inside a [nested] object"));
    }

    public void testValidateMultiValueArrayIndexVersionTooOldRejected() {
        var builder = new TestBuilder("field");
        var context = MapperBuilderContext.root(false, false);

        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> FieldArrayContext.validateMultiValueArray(
                "field",
                context,
                true,
                builder,
                IndexVersions.V_8_0_0,
                IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_KEYWORD
            )
        );
        assertThat(
            ex.getMessage(),
            containsString("[multi_value=array] which requires an index created on a version that supports natively storing arrays")
        );
    }

    public void testValidateMultiValueArrayHappyPath() {
        var builder = new TestBuilder("field");
        var context = MapperBuilderContext.root(false, false);

        // No exception expected on a well-formed configuration.
        FieldArrayContext.validateMultiValueArray(
            "field",
            context,
            true,
            builder,
            IndexVersion.current(),
            IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_KEYWORD
        );
    }

    /** Minimal {@link FieldMapper.Builder} stub used to drive validator and helper tests without dragging in field-type plumbing. */
    private static final class TestBuilder extends FieldMapper.Builder {

        TestBuilder(String name) {
            super(name);
        }

        @Override
        protected FieldMapper.Parameter<?>[] getParameters() {
            return new FieldMapper.Parameter<?>[0];
        }

        @Override
        public String contentType() {
            return "test";
        }

        @Override
        public FieldMapper build(MapperBuilderContext context) {
            throw new UnsupportedOperationException("test stub");
        }
    }
}
