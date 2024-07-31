/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Optional;

import static org.elasticsearch.index.mapper.MapperService.MergeReason.INDEX_TEMPLATE;
import static org.elasticsearch.index.mapper.MapperService.MergeReason.MAPPING_UPDATE;

public final class ObjectMapperMergeTests extends ESTestCase {

    private final RootObjectMapper rootObjectMapper = createMapping(false, true, true, false);

    private RootObjectMapper createMapping(
        boolean disabledFieldEnabled,
        boolean fooFieldEnabled,
        boolean includeBarField,
        boolean includeBazField
    ) {
        RootObjectMapper.Builder rootBuilder = new RootObjectMapper.Builder("type1", Optional.empty());
        rootBuilder.add(new ObjectMapper.Builder("disabled", Optional.empty()).enabled(disabledFieldEnabled));
        ObjectMapper.Builder fooBuilder = new ObjectMapper.Builder("foo", Optional.empty()).enabled(fooFieldEnabled);
        if (includeBarField) {
            fooBuilder.add(new TextFieldMapper.Builder("bar", createDefaultIndexAnalyzers(), false));
        }
        if (includeBazField) {
            fooBuilder.add(new TextFieldMapper.Builder("baz", createDefaultIndexAnalyzers(), false));
        }
        rootBuilder.add(fooBuilder);
        return rootBuilder.build(MapperBuilderContext.root(false, false));
    }

    public void testMerge() {
        // GIVEN an enriched mapping with "baz" new field
        ObjectMapper mergeWith = createMapping(false, true, true, true);

        // WHEN merging mappings
        final ObjectMapper merged = rootObjectMapper.merge(
            mergeWith,
            MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE)
        );

        // THEN "baz" new field is added to merged mapping
        final ObjectMapper mergedFoo = (ObjectMapper) merged.getMapper("foo");
        {
            Mapper bar = mergedFoo.getMapper("bar");
            assertEquals("bar", bar.leafName());
            assertEquals("foo.bar", bar.fullPath());
            Mapper baz = mergedFoo.getMapper("baz");
            assertEquals("baz", baz.leafName());
            assertEquals("foo.baz", baz.fullPath());
        }
    }

    public void testMergeWhenDisablingField() {
        // GIVEN a mapping with "foo" field disabled
        ObjectMapper mergeWith = createMapping(false, false, false, false);

        // WHEN merging mappings
        // THEN a MapperException is thrown with an excepted message
        MapperException e = expectThrows(
            MapperException.class,
            () -> rootObjectMapper.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE))
        );
        assertEquals("the [enabled] parameter can't be updated for the object mapping [foo]", e.getMessage());
    }

    public void testMergeDisabledField() {
        // GIVEN a mapping with "foo" field disabled
        // the field is disabled, and we are not trying to re-enable it, hence merge should work
        RootObjectMapper mergeWith = new RootObjectMapper.Builder("_doc", Optional.empty()).add(
            new ObjectMapper.Builder("disabled", Optional.empty())
        ).build(MapperBuilderContext.root(false, false));

        RootObjectMapper merged = rootObjectMapper.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE));
        assertFalse(((ObjectMapper) merged.getMapper("disabled")).isEnabled());
    }

    public void testMergeEnabled() {
        ObjectMapper mergeWith = createMapping(true, true, true, false);

        MapperException e = expectThrows(
            MapperException.class,
            () -> rootObjectMapper.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE))
        );
        assertEquals("the [enabled] parameter can't be updated for the object mapping [disabled]", e.getMessage());

        ObjectMapper result = rootObjectMapper.merge(mergeWith, MapperMergeContext.root(false, false, INDEX_TEMPLATE, Long.MAX_VALUE));
        assertTrue(result.isEnabled());
    }

    public void testMergeEnabledForRootMapper() {
        String type = MapperService.SINGLE_MAPPING_NAME;
        ObjectMapper firstMapper = new RootObjectMapper.Builder("_doc", Optional.empty()).build(MapperBuilderContext.root(false, false));
        ObjectMapper secondMapper = new RootObjectMapper.Builder("_doc", Optional.empty()).enabled(false)
            .build(MapperBuilderContext.root(false, false));

        MapperException e = expectThrows(
            MapperException.class,
            () -> firstMapper.merge(secondMapper, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE))
        );
        assertEquals("the [enabled] parameter can't be updated for the object mapping [" + type + "]", e.getMessage());

        ObjectMapper result = firstMapper.merge(secondMapper, MapperMergeContext.root(false, false, INDEX_TEMPLATE, Long.MAX_VALUE));
        assertFalse(result.isEnabled());
    }

    public void testMergeDisabledRootMapper() {
        String type = MapperService.SINGLE_MAPPING_NAME;
        final RootObjectMapper rootObjectMapper = (RootObjectMapper) new RootObjectMapper.Builder(type, ObjectMapper.Defaults.SUBOBJECTS)
            .enabled(false)
            .build(MapperBuilderContext.root(false, false));
        // the root is disabled, and we are not trying to re-enable it, but we do want to be able to add runtime fields
        final RootObjectMapper mergeWith = new RootObjectMapper.Builder(type, ObjectMapper.Defaults.SUBOBJECTS).addRuntimeFields(
            Collections.singletonMap("test", new TestRuntimeField("test", "long"))
        ).build(MapperBuilderContext.root(false, false));

        RootObjectMapper merged = rootObjectMapper.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE));
        assertFalse(merged.isEnabled());
        assertEquals(1, merged.runtimeFields().size());
        assertEquals("test", merged.runtimeFields().iterator().next().name());
    }

    public void testMergedFieldNamesFieldWithDotsSubobjectsFalseAtRoot() {
        RootObjectMapper mergeInto = createRootSubobjectFalseLeafWithDots();
        RootObjectMapper mergeWith = createRootSubobjectFalseLeafWithDots();

        final ObjectMapper merged = mergeInto.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE));

        final KeywordFieldMapper keywordFieldMapper = (KeywordFieldMapper) merged.getMapper("host.name");
        assertEquals("host.name", keywordFieldMapper.fullPath());
        assertEquals("host.name", keywordFieldMapper.leafName());
    }

    public void testMergedFieldNamesFieldWithDotsSubobjectsFalse() {
        RootObjectMapper mergeInto = new RootObjectMapper.Builder("_doc", Optional.empty()).add(createObjectSubobjectsFalseLeafWithDots())
            .build(MapperBuilderContext.root(false, false));
        RootObjectMapper mergeWith = new RootObjectMapper.Builder("_doc", Optional.empty()).add(createObjectSubobjectsFalseLeafWithDots())
            .build(MapperBuilderContext.root(false, false));

        final ObjectMapper merged = mergeInto.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE));

        ObjectMapper foo = (ObjectMapper) merged.getMapper("foo");
        ObjectMapper metrics = (ObjectMapper) foo.getMapper("metrics");
        final KeywordFieldMapper keywordFieldMapper = (KeywordFieldMapper) metrics.getMapper("host.name");
        assertEquals("foo.metrics.host.name", keywordFieldMapper.fullPath());
        assertEquals("host.name", keywordFieldMapper.leafName());
    }

    public void testMergedFieldNamesMultiFields() {
        RootObjectMapper mergeInto = new RootObjectMapper.Builder("_doc", Optional.empty()).add(createTextKeywordMultiField("text"))
            .build(MapperBuilderContext.root(false, false));
        RootObjectMapper mergeWith = new RootObjectMapper.Builder("_doc", Optional.empty()).add(createTextKeywordMultiField("text"))
            .build(MapperBuilderContext.root(false, false));

        final ObjectMapper merged = mergeInto.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE));

        TextFieldMapper text = (TextFieldMapper) merged.getMapper("text");
        assertEquals("text", text.fullPath());
        assertEquals("text", text.leafName());
        KeywordFieldMapper keyword = (KeywordFieldMapper) text.multiFields().iterator().next();
        assertEquals("text.keyword", keyword.fullPath());
        assertEquals("keyword", keyword.leafName());
    }

    public void testMergedFieldNamesMultiFieldsWithinSubobjectsFalse() {
        RootObjectMapper mergeInto = new RootObjectMapper.Builder("_doc", Optional.empty()).add(
            createObjectSubobjectsFalseLeafWithMultiField()
        ).build(MapperBuilderContext.root(false, false));
        RootObjectMapper mergeWith = new RootObjectMapper.Builder("_doc", Optional.empty()).add(
            createObjectSubobjectsFalseLeafWithMultiField()
        ).build(MapperBuilderContext.root(false, false));

        final ObjectMapper merged = mergeInto.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE));

        ObjectMapper foo = (ObjectMapper) merged.getMapper("foo");
        ObjectMapper metrics = (ObjectMapper) foo.getMapper("metrics");
        final TextFieldMapper textFieldMapper = (TextFieldMapper) metrics.getMapper("host.name");
        assertEquals("foo.metrics.host.name", textFieldMapper.fullPath());
        assertEquals("host.name", textFieldMapper.leafName());
        FieldMapper fieldMapper = textFieldMapper.multiFields.iterator().next();
        assertEquals("foo.metrics.host.name.keyword", fieldMapper.fullPath());
        assertEquals("keyword", fieldMapper.leafName());
    }

    public void testMergeWithLimit() {
        // GIVEN an enriched mapping with "baz" new field
        ObjectMapper mergeWith = createMapping(false, true, true, true);

        // WHEN merging mappings
        final ObjectMapper mergedAdd0 = rootObjectMapper.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, 0));
        final ObjectMapper mergedAdd1 = rootObjectMapper.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, 1));

        // THEN "baz" new field is added to merged mapping
        assertEquals(3, rootObjectMapper.getTotalFieldsCount());
        assertEquals(4, mergeWith.getTotalFieldsCount());
        assertEquals(3, mergedAdd0.getTotalFieldsCount());
        assertEquals(4, mergedAdd1.getTotalFieldsCount());
    }

    public void testMergeWithLimitTruncatedObjectField() {
        RootObjectMapper root = new RootObjectMapper.Builder("_doc", Optional.empty()).build(MapperBuilderContext.root(false, false));
        RootObjectMapper mergeWith = new RootObjectMapper.Builder("_doc", Optional.empty()).add(
            new ObjectMapper.Builder("parent", Optional.of(ObjectMapper.Subobjects.DISABLED)).add(
                new KeywordFieldMapper.Builder("child1", IndexVersion.current())
            ).add(new KeywordFieldMapper.Builder("child2", IndexVersion.current()))
        ).build(MapperBuilderContext.root(false, false));

        ObjectMapper mergedAdd0 = root.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, 0));
        ObjectMapper mergedAdd1 = root.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, 1));
        ObjectMapper mergedAdd2 = root.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, 2));
        ObjectMapper mergedAdd3 = root.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, 3));
        assertEquals(0, root.getTotalFieldsCount());
        assertEquals(0, mergedAdd0.getTotalFieldsCount());
        assertEquals(1, mergedAdd1.getTotalFieldsCount());
        assertEquals(2, mergedAdd2.getTotalFieldsCount());
        assertEquals(3, mergedAdd3.getTotalFieldsCount());

        ObjectMapper parent1 = (ObjectMapper) mergedAdd1.getMapper("parent");
        assertNull(parent1.getMapper("child1"));
        assertNull(parent1.getMapper("child2"));

        ObjectMapper parent2 = (ObjectMapper) mergedAdd2.getMapper("parent");
        // the order is not deterministic, but we expect one to be null and the other to be non-null
        assertTrue(parent2.getMapper("child1") == null ^ parent2.getMapper("child2") == null);

        ObjectMapper parent3 = (ObjectMapper) mergedAdd3.getMapper("parent");
        assertNotNull(parent3.getMapper("child1"));
        assertNotNull(parent3.getMapper("child2"));
    }

    public void testMergeSameObjectDifferentFields() {
        RootObjectMapper root = new RootObjectMapper.Builder("_doc", Optional.empty()).add(
            new ObjectMapper.Builder("parent", Optional.empty()).add(new KeywordFieldMapper.Builder("child1", IndexVersion.current()))
        ).build(MapperBuilderContext.root(false, false));
        RootObjectMapper mergeWith = new RootObjectMapper.Builder("_doc", Optional.empty()).add(
            new ObjectMapper.Builder("parent", Optional.empty()).add(
                new KeywordFieldMapper.Builder("child1", IndexVersion.current()).ignoreAbove(42)
            ).add(new KeywordFieldMapper.Builder("child2", IndexVersion.current()))
        ).build(MapperBuilderContext.root(false, false));

        ObjectMapper mergedAdd0 = root.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, 0));
        ObjectMapper mergedAdd1 = root.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, 1));
        assertEquals(2, root.getTotalFieldsCount());
        assertEquals(2, mergedAdd0.getTotalFieldsCount());
        assertEquals(3, mergedAdd1.getTotalFieldsCount());

        ObjectMapper parent0 = (ObjectMapper) mergedAdd0.getMapper("parent");
        assertNotNull(parent0.getMapper("child1"));
        assertEquals(42, ((KeywordFieldMapper) parent0.getMapper("child1")).fieldType().ignoreAbove());
        assertNull(parent0.getMapper("child2"));

        ObjectMapper parent1 = (ObjectMapper) mergedAdd1.getMapper("parent");
        assertNotNull(parent1.getMapper("child1"));
        assertEquals(42, ((KeywordFieldMapper) parent1.getMapper("child1")).fieldType().ignoreAbove());
        assertNotNull(parent1.getMapper("child2"));
    }

    public void testMergeWithLimitMultiField() {
        RootObjectMapper mergeInto = new RootObjectMapper.Builder("_doc", Optional.empty()).add(
            createTextKeywordMultiField("text", "keyword1")
        ).build(MapperBuilderContext.root(false, false));
        RootObjectMapper mergeWith = new RootObjectMapper.Builder("_doc", Optional.empty()).add(
            createTextKeywordMultiField("text", "keyword2")
        ).build(MapperBuilderContext.root(false, false));

        assertEquals(2, mergeInto.getTotalFieldsCount());
        assertEquals(2, mergeWith.getTotalFieldsCount());

        ObjectMapper mergedAdd0 = mergeInto.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, 0));
        ObjectMapper mergedAdd1 = mergeInto.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, 1));
        assertEquals(2, mergedAdd0.getTotalFieldsCount());
        assertEquals(3, mergedAdd1.getTotalFieldsCount());
    }

    public void testMergeWithLimitRuntimeField() {
        RootObjectMapper mergeInto = new RootObjectMapper.Builder("_doc", Optional.empty()).addRuntimeField(
            new TestRuntimeField("existing_runtime_field", "keyword")
        ).add(createTextKeywordMultiField("text", "keyword1")).build(MapperBuilderContext.root(false, false));
        RootObjectMapper mergeWith = new RootObjectMapper.Builder("_doc", Optional.empty()).addRuntimeField(
            new TestRuntimeField("existing_runtime_field", "keyword")
        ).addRuntimeField(new TestRuntimeField("new_runtime_field", "keyword")).build(MapperBuilderContext.root(false, false));

        assertEquals(3, mergeInto.getTotalFieldsCount());
        assertEquals(2, mergeWith.getTotalFieldsCount());

        ObjectMapper mergedAdd0 = mergeInto.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, 0));
        ObjectMapper mergedAdd1 = mergeInto.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, 1));
        assertEquals(3, mergedAdd0.getTotalFieldsCount());
        assertEquals(4, mergedAdd1.getTotalFieldsCount());
    }

    public void testMergeSubobjectsFalseWithObject() {
        RootObjectMapper mergeInto = new RootObjectMapper.Builder("_doc", Optional.empty()).add(
            new ObjectMapper.Builder("parent", Optional.of(ObjectMapper.Subobjects.DISABLED))
        ).build(MapperBuilderContext.root(false, false));
        RootObjectMapper mergeWith = new RootObjectMapper.Builder("_doc", Optional.empty()).add(
            new ObjectMapper.Builder("parent", Optional.empty()).add(
                new ObjectMapper.Builder("child", Optional.empty()).add(
                    new KeywordFieldMapper.Builder("grandchild", IndexVersion.current())
                )
            )
        ).build(MapperBuilderContext.root(false, false));

        ObjectMapper merged = mergeInto.merge(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE));
        ObjectMapper parentMapper = (ObjectMapper) merged.getMapper("parent");
        assertNotNull(parentMapper);
        assertNotNull(parentMapper.getMapper("child.grandchild"));
    }

    private static RootObjectMapper createRootSubobjectFalseLeafWithDots() {
        FieldMapper.Builder fieldBuilder = new KeywordFieldMapper.Builder("host.name", IndexVersion.current());
        FieldMapper fieldMapper = fieldBuilder.build(MapperBuilderContext.root(false, false));
        assertEquals("host.name", fieldMapper.leafName());
        assertEquals("host.name", fieldMapper.fullPath());
        return new RootObjectMapper.Builder("_doc", Optional.of(ObjectMapper.Subobjects.DISABLED)).add(fieldBuilder)
            .build(MapperBuilderContext.root(false, false));
    }

    private static ObjectMapper.Builder createObjectSubobjectsFalseLeafWithDots() {
        KeywordFieldMapper.Builder fieldBuilder = new KeywordFieldMapper.Builder("host.name", IndexVersion.current());
        KeywordFieldMapper fieldMapper = fieldBuilder.build(
            new MapperBuilderContext(
                "foo.metrics",
                false,
                false,
                false,
                ObjectMapper.Defaults.DYNAMIC,
                MapperService.MergeReason.MAPPING_UPDATE,
                false
            )
        );
        assertEquals("host.name", fieldMapper.leafName());
        assertEquals("foo.metrics.host.name", fieldMapper.fullPath());
        return new ObjectMapper.Builder("foo", ObjectMapper.Defaults.SUBOBJECTS).add(
            new ObjectMapper.Builder("metrics", Optional.of(ObjectMapper.Subobjects.DISABLED)).add(fieldBuilder)
        );
    }

    private ObjectMapper.Builder createObjectSubobjectsFalseLeafWithMultiField() {
        TextFieldMapper.Builder fieldBuilder = createTextKeywordMultiField("host.name");
        TextFieldMapper textKeywordMultiField = fieldBuilder.build(
            new MapperBuilderContext(
                "foo.metrics",
                false,
                false,
                false,
                ObjectMapper.Defaults.DYNAMIC,
                MapperService.MergeReason.MAPPING_UPDATE,
                false
            )
        );
        assertEquals("host.name", textKeywordMultiField.leafName());
        assertEquals("foo.metrics.host.name", textKeywordMultiField.fullPath());
        FieldMapper fieldMapper = textKeywordMultiField.multiFields.iterator().next();
        assertEquals("keyword", fieldMapper.leafName());
        assertEquals("foo.metrics.host.name.keyword", fieldMapper.fullPath());
        return new ObjectMapper.Builder("foo", ObjectMapper.Defaults.SUBOBJECTS).add(
            new ObjectMapper.Builder("metrics", Optional.of(ObjectMapper.Subobjects.DISABLED)).add(fieldBuilder)
        );
    }

    private TextFieldMapper.Builder createTextKeywordMultiField(String name) {
        return createTextKeywordMultiField(name, "keyword");
    }

    private TextFieldMapper.Builder createTextKeywordMultiField(String name, String multiFieldName) {
        TextFieldMapper.Builder builder = new TextFieldMapper.Builder(name, createDefaultIndexAnalyzers(), false);
        builder.multiFieldsBuilder.add(new KeywordFieldMapper.Builder(multiFieldName, IndexVersion.current()));
        return builder;
    }
}
