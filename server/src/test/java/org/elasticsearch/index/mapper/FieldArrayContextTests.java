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
        MapperBuilderContext nestedSynthetic = new MapperBuilderContext(
            null,
            true,
            false,
            false,
            ObjectMapper.Defaults.DYNAMIC,
            MapperService.MergeReason.MAPPING_UPDATE,
            true
        );

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
        // no doc values
        assertNull(
            getOffsetsFieldName(
                syntheticRoot,
                Mapper.SourceKeepMode.NONE,
                false,
                false,
                builder,
                IndexVersion.current(),
                IndexVersions.MINIMUM_COMPATIBLE,
                true,
                true
            )
        );
        // stored
        assertNull(
            getOffsetsFieldName(
                syntheticRoot,
                Mapper.SourceKeepMode.NONE,
                true,
                true,
                builder,
                IndexVersion.current(),
                IndexVersions.MINIMUM_COMPATIBLE,
                true,
                true
            )
        );
        // nested context
        assertNull(
            getOffsetsFieldName(
                nestedSynthetic,
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
    }

    public void testGetOffsetsFieldNameColumnarBranchIgnoresCopyToAndMultiFields() {
        // copy_to set should not block the new branch
        FieldMapper.Builder withCopyTo = newTestBuilder("field");
        withCopyTo.copyTo = FieldMapper.CopyTo.empty().withAddedFields(List.of("target"));
        MapperBuilderContext context = MapperBuilderContext.root(true, false);
        assertEquals(
            "field" + FieldArrayContext.OFFSETS_FIELD_NAME_SUFFIX,
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

        // multi-fields should not block the new branch
        FieldMapper.Builder withMultiFields = newTestBuilder("field");
        withMultiFields.multiFieldsBuilder.add(newTestBuilder("sub"));
        assertEquals(
            "field" + FieldArrayContext.OFFSETS_FIELD_NAME_SUFFIX,
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
