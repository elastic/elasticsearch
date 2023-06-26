/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Explicit;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

public class ObjectMapperMergeTests extends ESTestCase {

    private final RootObjectMapper rootObjectMapper = createMapping(false, true, true, false);

    private RootObjectMapper createMapping(
        boolean disabledFieldEnabled,
        boolean fooFieldEnabled,
        boolean includeBarField,
        boolean includeBazField
    ) {
        RootObjectMapper.Builder rootBuilder = new RootObjectMapper.Builder("type1", Explicit.IMPLICIT_TRUE);
        rootBuilder.add(new ObjectMapper.Builder("disabled", Explicit.IMPLICIT_TRUE).enabled(disabledFieldEnabled));
        ObjectMapper.Builder fooBuilder = new ObjectMapper.Builder("foo", Explicit.IMPLICIT_TRUE).enabled(fooFieldEnabled);
        if (includeBarField) {
            fooBuilder.add(new TextFieldMapper.Builder("bar", createDefaultIndexAnalyzers()));
        }
        if (includeBazField) {
            fooBuilder.add(new TextFieldMapper.Builder("baz", createDefaultIndexAnalyzers()));
        }
        rootBuilder.add(fooBuilder);
        return rootBuilder.build(MapperBuilderContext.root(false));
    }

    public void testMerge() {
        // GIVEN an enriched mapping with "baz" new field
        ObjectMapper mergeWith = createMapping(false, true, true, true);

        // WHEN merging mappings
        final ObjectMapper merged = rootObjectMapper.merge(mergeWith, MapperBuilderContext.root(false));

        // THEN "baz" new field is added to merged mapping
        final ObjectMapper mergedFoo = (ObjectMapper) merged.getMapper("foo");
        {
            Mapper bar = mergedFoo.getMapper("bar");
            assertEquals("bar", bar.simpleName());
            assertEquals("foo.bar", bar.name());
            Mapper baz = mergedFoo.getMapper("baz");
            assertEquals("baz", baz.simpleName());
            assertEquals("foo.baz", baz.name());
        }
    }

    public void testMergeWhenDisablingField() {
        // GIVEN a mapping with "foo" field disabled
        ObjectMapper mergeWith = createMapping(false, false, false, false);

        // WHEN merging mappings
        // THEN a MapperException is thrown with an excepted message
        MapperException e = expectThrows(MapperException.class, () -> rootObjectMapper.merge(mergeWith, MapperBuilderContext.root(false)));
        assertEquals("the [enabled] parameter can't be updated for the object mapping [foo]", e.getMessage());
    }

    public void testMergeDisabledField() {
        // GIVEN a mapping with "foo" field disabled
        // the field is disabled, and we are not trying to re-enable it, hence merge should work
        RootObjectMapper mergeWith = new RootObjectMapper.Builder("_doc", Explicit.IMPLICIT_TRUE).add(
            new ObjectMapper.Builder("disabled", Explicit.IMPLICIT_TRUE)
        ).build(MapperBuilderContext.root(false));

        RootObjectMapper merged = (RootObjectMapper) rootObjectMapper.merge(mergeWith, MapperBuilderContext.root(false));
        assertFalse(((ObjectMapper) merged.getMapper("disabled")).isEnabled());
    }

    public void testMergeEnabled() {
        ObjectMapper mergeWith = createMapping(true, true, true, false);

        MapperException e = expectThrows(MapperException.class, () -> rootObjectMapper.merge(mergeWith, MapperBuilderContext.root(false)));
        assertEquals("the [enabled] parameter can't be updated for the object mapping [disabled]", e.getMessage());

        ObjectMapper result = rootObjectMapper.merge(mergeWith, MapperService.MergeReason.INDEX_TEMPLATE, MapperBuilderContext.root(false));
        assertTrue(result.isEnabled());
    }

    public void testMergeEnabledForRootMapper() {
        String type = MapperService.SINGLE_MAPPING_NAME;
        ObjectMapper firstMapper = new RootObjectMapper.Builder("_doc", Explicit.IMPLICIT_TRUE).build(MapperBuilderContext.root(false));
        ObjectMapper secondMapper = new RootObjectMapper.Builder("_doc", Explicit.IMPLICIT_TRUE).enabled(false)
            .build(MapperBuilderContext.root(false));

        MapperException e = expectThrows(MapperException.class, () -> firstMapper.merge(secondMapper, MapperBuilderContext.root(false)));
        assertEquals("the [enabled] parameter can't be updated for the object mapping [" + type + "]", e.getMessage());

        ObjectMapper result = firstMapper.merge(secondMapper, MapperService.MergeReason.INDEX_TEMPLATE, MapperBuilderContext.root(false));
        assertFalse(result.isEnabled());
    }

    public void testMergeDisabledRootMapper() {
        String type = MapperService.SINGLE_MAPPING_NAME;
        final RootObjectMapper rootObjectMapper = (RootObjectMapper) new RootObjectMapper.Builder(type, ObjectMapper.Defaults.SUBOBJECTS)
            .enabled(false)
            .build(MapperBuilderContext.root(false));
        // the root is disabled, and we are not trying to re-enable it, but we do want to be able to add runtime fields
        final RootObjectMapper mergeWith = new RootObjectMapper.Builder(type, ObjectMapper.Defaults.SUBOBJECTS).addRuntimeFields(
            Collections.singletonMap("test", new TestRuntimeField("test", "long"))
        ).build(MapperBuilderContext.root(false));

        RootObjectMapper merged = (RootObjectMapper) rootObjectMapper.merge(mergeWith, MapperBuilderContext.root(false));
        assertFalse(merged.isEnabled());
        assertEquals(1, merged.runtimeFields().size());
        assertEquals("test", merged.runtimeFields().iterator().next().name());
    }

    public void testMergedFieldNamesFieldWithDotsSubobjectsFalseAtRoot() {
        RootObjectMapper mergeInto = createRootSubobjectFalseLeafWithDots();
        RootObjectMapper mergeWith = createRootSubobjectFalseLeafWithDots();

        final ObjectMapper merged = mergeInto.merge(mergeWith, MapperBuilderContext.root(false));

        final KeywordFieldMapper keywordFieldMapper = (KeywordFieldMapper) merged.getMapper("host.name");
        assertEquals("host.name", keywordFieldMapper.name());
        assertEquals("host.name", keywordFieldMapper.simpleName());
    }

    public void testMergedFieldNamesFieldWithDotsSubobjectsFalse() {
        RootObjectMapper mergeInto = new RootObjectMapper.Builder("_doc", Explicit.IMPLICIT_TRUE).add(
            createObjectSubobjectsFalseLeafWithDots()
        ).build(MapperBuilderContext.root(false));
        RootObjectMapper mergeWith = new RootObjectMapper.Builder("_doc", Explicit.IMPLICIT_TRUE).add(
            createObjectSubobjectsFalseLeafWithDots()
        ).build(MapperBuilderContext.root(false));

        final ObjectMapper merged = mergeInto.merge(mergeWith, MapperBuilderContext.root(false));

        ObjectMapper foo = (ObjectMapper) merged.getMapper("foo");
        ObjectMapper metrics = (ObjectMapper) foo.getMapper("metrics");
        final KeywordFieldMapper keywordFieldMapper = (KeywordFieldMapper) metrics.getMapper("host.name");
        assertEquals("foo.metrics.host.name", keywordFieldMapper.name());
        assertEquals("host.name", keywordFieldMapper.simpleName());
    }

    public void testMergedFieldNamesMultiFields() {
        RootObjectMapper mergeInto = new RootObjectMapper.Builder("_doc", Explicit.IMPLICIT_TRUE).add(createTextKeywordMultiField("text"))
            .build(MapperBuilderContext.root(false));
        RootObjectMapper mergeWith = new RootObjectMapper.Builder("_doc", Explicit.IMPLICIT_TRUE).add(createTextKeywordMultiField("text"))
            .build(MapperBuilderContext.root(false));

        final ObjectMapper merged = mergeInto.merge(mergeWith, MapperBuilderContext.root(false));

        TextFieldMapper text = (TextFieldMapper) merged.getMapper("text");
        assertEquals("text", text.name());
        assertEquals("text", text.simpleName());
        KeywordFieldMapper keyword = (KeywordFieldMapper) text.multiFields().iterator().next();
        assertEquals("text.keyword", keyword.name());
        assertEquals("keyword", keyword.simpleName());
    }

    public void testMergedFieldNamesMultiFieldsWithinSubobjectsFalse() {
        RootObjectMapper mergeInto = new RootObjectMapper.Builder("_doc", Explicit.IMPLICIT_TRUE).add(
            createObjectSubobjectsFalseLeafWithMultiField()
        ).build(MapperBuilderContext.root(false));
        RootObjectMapper mergeWith = new RootObjectMapper.Builder("_doc", Explicit.IMPLICIT_TRUE).add(
            createObjectSubobjectsFalseLeafWithMultiField()
        ).build(MapperBuilderContext.root(false));

        final ObjectMapper merged = mergeInto.merge(mergeWith, MapperBuilderContext.root(false));

        ObjectMapper foo = (ObjectMapper) merged.getMapper("foo");
        ObjectMapper metrics = (ObjectMapper) foo.getMapper("metrics");
        final TextFieldMapper textFieldMapper = (TextFieldMapper) metrics.getMapper("host.name");
        assertEquals("foo.metrics.host.name", textFieldMapper.name());
        assertEquals("host.name", textFieldMapper.simpleName());
        FieldMapper fieldMapper = textFieldMapper.multiFields.iterator().next();
        assertEquals("foo.metrics.host.name.keyword", fieldMapper.name());
        assertEquals("keyword", fieldMapper.simpleName());
    }

    private static RootObjectMapper createRootSubobjectFalseLeafWithDots() {
        FieldMapper.Builder fieldBuilder = new KeywordFieldMapper.Builder("host.name", IndexVersion.CURRENT);
        FieldMapper fieldMapper = fieldBuilder.build(MapperBuilderContext.root(false));
        assertEquals("host.name", fieldMapper.simpleName());
        assertEquals("host.name", fieldMapper.name());
        return new RootObjectMapper.Builder("_doc", Explicit.EXPLICIT_FALSE).add(fieldBuilder).build(MapperBuilderContext.root(false));
    }

    private static ObjectMapper.Builder createObjectSubobjectsFalseLeafWithDots() {
        KeywordFieldMapper.Builder fieldBuilder = new KeywordFieldMapper.Builder("host.name", IndexVersion.CURRENT);
        KeywordFieldMapper fieldMapper = fieldBuilder.build(new MapperBuilderContext("foo.metrics", false));
        assertEquals("host.name", fieldMapper.simpleName());
        assertEquals("foo.metrics.host.name", fieldMapper.name());
        return new ObjectMapper.Builder("foo", ObjectMapper.Defaults.SUBOBJECTS).add(
            new ObjectMapper.Builder("metrics", Explicit.EXPLICIT_FALSE).add(fieldBuilder)
        );
    }

    private ObjectMapper.Builder createObjectSubobjectsFalseLeafWithMultiField() {
        TextFieldMapper.Builder fieldBuilder = createTextKeywordMultiField("host.name");
        TextFieldMapper textKeywordMultiField = fieldBuilder.build(new MapperBuilderContext("foo.metrics", false));
        assertEquals("host.name", textKeywordMultiField.simpleName());
        assertEquals("foo.metrics.host.name", textKeywordMultiField.name());
        FieldMapper fieldMapper = textKeywordMultiField.multiFields.iterator().next();
        assertEquals("keyword", fieldMapper.simpleName());
        assertEquals("foo.metrics.host.name.keyword", fieldMapper.name());
        return new ObjectMapper.Builder("foo", ObjectMapper.Defaults.SUBOBJECTS).add(
            new ObjectMapper.Builder("metrics", Explicit.EXPLICIT_FALSE).add(fieldBuilder)
        );
    }

    private TextFieldMapper.Builder createTextKeywordMultiField(String name) {
        TextFieldMapper.Builder builder = new TextFieldMapper.Builder(name, createDefaultIndexAnalyzers());
        builder.multiFieldsBuilder.add(new KeywordFieldMapper.Builder("keyword", IndexVersion.CURRENT));
        return builder;
    }
}
