/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Explicit;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.elasticsearch.index.mapper.MapperService.MergeReason.INDEX_TEMPLATE;
import static org.elasticsearch.index.mapper.MapperService.MergeReason.MAPPING_AUTO_UPDATE;
import static org.elasticsearch.index.mapper.MapperService.MergeReason.MAPPING_AUTO_UPDATE_PREFLIGHT;
import static org.elasticsearch.index.mapper.MapperService.MergeReason.MAPPING_UPDATE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public final class ObjectMapperMergeTests extends ESTestCase {

    private final IndexSettings INDEX_SETTINGS = defaultIndexSettings();

    private RootObjectMapper.Builder createMappingBuilder(
        boolean disabledFieldEnabled,
        boolean fooFieldEnabled,
        boolean includeBarField,
        boolean includeBazField
    ) {
        RootObjectMapper.Builder rootBuilder = new RootObjectMapper.Builder("type1");
        rootBuilder.add(new ObjectMapper.Builder("disabled").enabled(disabledFieldEnabled));
        ObjectMapper.Builder fooBuilder = new ObjectMapper.Builder("foo").enabled(fooFieldEnabled);
        if (includeBarField) {
            fooBuilder.add(new TextFieldMapper.Builder("bar", createDefaultIndexAnalyzers()));
        }
        if (includeBazField) {
            fooBuilder.add(new TextFieldMapper.Builder("baz", createDefaultIndexAnalyzers()));
        }
        rootBuilder.add(fooBuilder);
        return rootBuilder;
    }

    private ObjectMapper mergeBuilders(ObjectMapper.Builder existing, ObjectMapper.Builder incoming, MapperMergeContext mergeContext) {
        return (ObjectMapper) existing.mergeWith(incoming, mergeContext).build(mergeContext.getMapperBuilderContext());
    }

    public void testMerge() {
        RootObjectMapper.Builder existing = createMappingBuilder(false, true, true, false);
        RootObjectMapper.Builder mergeWith = createMappingBuilder(false, true, true, true);

        final ObjectMapper merged = mergeBuilders(
            existing,
            mergeWith,
            MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE)
        );

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
        RootObjectMapper.Builder existing = createMappingBuilder(false, true, true, false);
        RootObjectMapper.Builder mergeWith = createMappingBuilder(false, false, false, false);

        MapperException e = expectThrows(
            MapperException.class,
            () -> existing.mergeWith(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE))
        );
        assertEquals("the [enabled] parameter can't be updated for the object mapping [foo]", e.getMessage());
    }

    public void testMergeDisabledField() {
        RootObjectMapper.Builder existing = createMappingBuilder(false, true, true, false);
        RootObjectMapper.Builder mergeWith = new RootObjectMapper.Builder("_doc").add(new ObjectMapper.Builder("disabled"));

        RootObjectMapper merged = (RootObjectMapper) mergeBuilders(
            existing,
            mergeWith,
            MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE)
        );
        assertFalse(((ObjectMapper) merged.getMapper("disabled")).isEnabled());
    }

    public void testMergeEnabled() {
        RootObjectMapper.Builder existing = createMappingBuilder(false, true, true, false);
        RootObjectMapper.Builder mergeWith = createMappingBuilder(true, true, true, false);

        MapperException e = expectThrows(
            MapperException.class,
            () -> existing.mergeWith(mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE))
        );
        assertEquals("the [enabled] parameter can't be updated for the object mapping [disabled]", e.getMessage());

        RootObjectMapper.Builder existing2 = createMappingBuilder(false, true, true, false);
        RootObjectMapper.Builder mergeWith2 = createMappingBuilder(true, true, true, false);
        ObjectMapper result = mergeBuilders(existing2, mergeWith2, MapperMergeContext.root(false, false, INDEX_TEMPLATE, Long.MAX_VALUE));
        assertTrue(result.isEnabled());
    }

    public void testMergeEnabledForRootMapper() {
        String type = MapperService.SINGLE_MAPPING_NAME;
        ObjectMapper.Builder firstBuilder = new RootObjectMapper.Builder("_doc");
        ObjectMapper.Builder secondBuilder = new RootObjectMapper.Builder("_doc").enabled(false);

        MapperException e = expectThrows(
            MapperException.class,
            () -> firstBuilder.mergeWith(secondBuilder, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE))
        );
        assertEquals("the [enabled] parameter can't be updated for the object mapping [" + type + "]", e.getMessage());

        ObjectMapper.Builder firstBuilder2 = new RootObjectMapper.Builder("_doc");
        ObjectMapper.Builder secondBuilder2 = new RootObjectMapper.Builder("_doc").enabled(false);
        ObjectMapper result = mergeBuilders(
            firstBuilder2,
            secondBuilder2,
            MapperMergeContext.root(false, false, INDEX_TEMPLATE, Long.MAX_VALUE)
        );
        assertFalse(result.isEnabled());
    }

    public void testMergeDisabledRootMapper() {
        String type = MapperService.SINGLE_MAPPING_NAME;
        ObjectMapper.Builder rootBuilder = new RootObjectMapper.Builder(type, ObjectMapper.Defaults.SUBOBJECTS).enabled(false);
        ObjectMapper.Builder mergeWithBuilder = new RootObjectMapper.Builder(type, ObjectMapper.Defaults.SUBOBJECTS).addRuntimeFields(
            Collections.singletonMap("test", new TestRuntimeField("test", "long"))
        );

        RootObjectMapper merged = (RootObjectMapper) mergeBuilders(
            rootBuilder,
            mergeWithBuilder,
            MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE)
        );
        assertFalse(merged.isEnabled());
        assertEquals(1, merged.runtimeFields().size());
        assertEquals("test", merged.runtimeFields().iterator().next().name());
    }

    public void testMergedFieldNamesFieldWithDotsSubobjectsFalseAtRoot() {
        RootObjectMapper.Builder mergeInto = createRootSubobjectFalseLeafWithDotsBuilder();
        RootObjectMapper.Builder mergeWith = createRootSubobjectFalseLeafWithDotsBuilder();

        final ObjectMapper merged = mergeBuilders(
            mergeInto,
            mergeWith,
            MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE)
        );

        final KeywordFieldMapper keywordFieldMapper = (KeywordFieldMapper) merged.getMapper("host.name");
        assertEquals("host.name", keywordFieldMapper.fullPath());
        assertEquals("host.name", keywordFieldMapper.leafName());
    }

    public void testMergedFieldNamesFieldWithDotsSubobjectsFalse() {
        RootObjectMapper.Builder mergeInto = new RootObjectMapper.Builder("_doc").add(createObjectSubobjectsFalseLeafWithDots());
        RootObjectMapper.Builder mergeWith = new RootObjectMapper.Builder("_doc").add(createObjectSubobjectsFalseLeafWithDots());

        final ObjectMapper merged = mergeBuilders(
            mergeInto,
            mergeWith,
            MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE)
        );

        ObjectMapper foo = (ObjectMapper) merged.getMapper("foo");
        ObjectMapper metrics = (ObjectMapper) foo.getMapper("metrics");
        final KeywordFieldMapper keywordFieldMapper = (KeywordFieldMapper) metrics.getMapper("host.name");
        assertEquals("foo.metrics.host.name", keywordFieldMapper.fullPath());
        assertEquals("host.name", keywordFieldMapper.leafName());
    }

    public void testMergedFieldNamesMultiFields() {
        RootObjectMapper.Builder mergeInto = new RootObjectMapper.Builder("_doc").add(createTextKeywordMultiField("text"));
        RootObjectMapper.Builder mergeWith = new RootObjectMapper.Builder("_doc").add(createTextKeywordMultiField("text"));

        final ObjectMapper merged = mergeBuilders(
            mergeInto,
            mergeWith,
            MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE)
        );

        TextFieldMapper text = (TextFieldMapper) merged.getMapper("text");
        assertEquals("text", text.fullPath());
        assertEquals("text", text.leafName());
        KeywordFieldMapper keyword = (KeywordFieldMapper) text.multiFields().iterator().next();
        assertEquals("text.keyword", keyword.fullPath());
        assertEquals("keyword", keyword.leafName());
    }

    public void testMergedFieldNamesMultiFieldsWithinSubobjectsFalse() {
        RootObjectMapper.Builder mergeInto = new RootObjectMapper.Builder("_doc").add(createObjectSubobjectsFalseLeafWithMultiField());
        RootObjectMapper.Builder mergeWith = new RootObjectMapper.Builder("_doc").add(createObjectSubobjectsFalseLeafWithMultiField());

        final ObjectMapper merged = mergeBuilders(
            mergeInto,
            mergeWith,
            MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE)
        );

        ObjectMapper foo = (ObjectMapper) merged.getMapper("foo");
        ObjectMapper metrics = (ObjectMapper) foo.getMapper("metrics");
        final TextFieldMapper textFieldMapper = (TextFieldMapper) metrics.getMapper("host.name");
        assertEquals("foo.metrics.host.name", textFieldMapper.fullPath());
        assertEquals("host.name", textFieldMapper.leafName());
        FieldMapper fieldMapper = textFieldMapper.multiFields().iterator().next();
        assertEquals("foo.metrics.host.name.keyword", fieldMapper.fullPath());
        assertEquals("keyword", fieldMapper.leafName());
    }

    public void testMergeWithLimit() {
        RootObjectMapper.Builder existing = createMappingBuilder(false, true, true, false);
        RootObjectMapper existingMapper = existing.build(MapperBuilderContext.root(false, false));
        RootObjectMapper.Builder mergeWith = createMappingBuilder(false, true, true, true);
        RootObjectMapper mergeWithMapper = mergeWith.build(MapperBuilderContext.root(false, false));

        RootObjectMapper.Builder existing0 = createMappingBuilder(false, true, true, false);
        RootObjectMapper.Builder mergeWith0 = createMappingBuilder(false, true, true, true);
        final ObjectMapper mergedAdd0 = mergeBuilders(existing0, mergeWith0, MapperMergeContext.root(false, false, MAPPING_UPDATE, 0));

        RootObjectMapper.Builder existing1 = createMappingBuilder(false, true, true, false);
        RootObjectMapper.Builder mergeWith1 = createMappingBuilder(false, true, true, true);
        final ObjectMapper mergedAdd1 = mergeBuilders(existing1, mergeWith1, MapperMergeContext.root(false, false, MAPPING_UPDATE, 1));

        assertEquals(3, existingMapper.getTotalFieldsCount());
        assertEquals(4, mergeWithMapper.getTotalFieldsCount());
        assertEquals(3, mergedAdd0.getTotalFieldsCount());
        assertEquals(4, mergedAdd1.getTotalFieldsCount());
    }

    public void testMergeWithLimitTruncatedObjectField() {
        RootObjectMapper.Builder rootBuilder = new RootObjectMapper.Builder("_doc");
        ObjectMapper.Builder mergeWithBuilder = new RootObjectMapper.Builder("_doc").add(
            new ObjectMapper.Builder("parent", Explicit.of(ObjectMapper.Subobjects.DISABLED)).add(
                new KeywordFieldMapper.Builder("child1", INDEX_SETTINGS)
            ).add(new KeywordFieldMapper.Builder("child2", INDEX_SETTINGS))
        );

        // Need fresh builders for each merge since mergeWith() mutates
        ObjectMapper mergedAdd0 = mergeBuilders(
            new RootObjectMapper.Builder("_doc"),
            new RootObjectMapper.Builder("_doc").add(
                new ObjectMapper.Builder("parent", Explicit.of(ObjectMapper.Subobjects.DISABLED)).add(
                    new KeywordFieldMapper.Builder("child1", INDEX_SETTINGS)
                ).add(new KeywordFieldMapper.Builder("child2", INDEX_SETTINGS))
            ),
            MapperMergeContext.root(false, false, MAPPING_UPDATE, 0)
        );
        ObjectMapper mergedAdd1 = mergeBuilders(
            new RootObjectMapper.Builder("_doc"),
            new RootObjectMapper.Builder("_doc").add(
                new ObjectMapper.Builder("parent", Explicit.of(ObjectMapper.Subobjects.DISABLED)).add(
                    new KeywordFieldMapper.Builder("child1", INDEX_SETTINGS)
                ).add(new KeywordFieldMapper.Builder("child2", INDEX_SETTINGS))
            ),
            MapperMergeContext.root(false, false, MAPPING_UPDATE, 1)
        );
        ObjectMapper mergedAdd2 = mergeBuilders(
            new RootObjectMapper.Builder("_doc"),
            new RootObjectMapper.Builder("_doc").add(
                new ObjectMapper.Builder("parent", Explicit.of(ObjectMapper.Subobjects.DISABLED)).add(
                    new KeywordFieldMapper.Builder("child1", INDEX_SETTINGS)
                ).add(new KeywordFieldMapper.Builder("child2", INDEX_SETTINGS))
            ),
            MapperMergeContext.root(false, false, MAPPING_UPDATE, 2)
        );
        ObjectMapper mergedAdd3 = mergeBuilders(
            new RootObjectMapper.Builder("_doc"),
            new RootObjectMapper.Builder("_doc").add(
                new ObjectMapper.Builder("parent", Explicit.of(ObjectMapper.Subobjects.DISABLED)).add(
                    new KeywordFieldMapper.Builder("child1", INDEX_SETTINGS)
                ).add(new KeywordFieldMapper.Builder("child2", INDEX_SETTINGS))
            ),
            MapperMergeContext.root(false, false, MAPPING_UPDATE, 3)
        );

        assertEquals(0, rootBuilder.build(MapperBuilderContext.root(false, false)).getTotalFieldsCount());
        assertEquals(0, mergedAdd0.getTotalFieldsCount());
        assertEquals(1, mergedAdd1.getTotalFieldsCount());
        assertEquals(2, mergedAdd2.getTotalFieldsCount());
        assertEquals(3, mergedAdd3.getTotalFieldsCount());

        ObjectMapper parent1 = (ObjectMapper) mergedAdd1.getMapper("parent");
        assertNull(parent1.getMapper("child1"));
        assertNull(parent1.getMapper("child2"));

        ObjectMapper parent2 = (ObjectMapper) mergedAdd2.getMapper("parent");
        assertTrue(parent2.getMapper("child1") == null ^ parent2.getMapper("child2") == null);

        ObjectMapper parent3 = (ObjectMapper) mergedAdd3.getMapper("parent");
        assertNotNull(parent3.getMapper("child1"));
        assertNotNull(parent3.getMapper("child2"));
    }

    public void testMergeSameObjectDifferentFields() {
        RootObjectMapper root = new RootObjectMapper.Builder("_doc").add(
            new ObjectMapper.Builder("parent").add(new KeywordFieldMapper.Builder("child1", INDEX_SETTINGS))
        ).build(MapperBuilderContext.root(false, false));

        ObjectMapper mergedAdd0 = mergeBuilders(
            new RootObjectMapper.Builder("_doc").add(
                new ObjectMapper.Builder("parent").add(new KeywordFieldMapper.Builder("child1", INDEX_SETTINGS))
            ),
            new RootObjectMapper.Builder("_doc").add(
                new ObjectMapper.Builder("parent").add(new KeywordFieldMapper.Builder("child1", INDEX_SETTINGS).ignoreAbove(42))
                    .add(new KeywordFieldMapper.Builder("child2", INDEX_SETTINGS))
            ),
            MapperMergeContext.root(false, false, MAPPING_UPDATE, 0)
        );
        ObjectMapper mergedAdd1 = mergeBuilders(
            new RootObjectMapper.Builder("_doc").add(
                new ObjectMapper.Builder("parent").add(new KeywordFieldMapper.Builder("child1", INDEX_SETTINGS))
            ),
            new RootObjectMapper.Builder("_doc").add(
                new ObjectMapper.Builder("parent").add(new KeywordFieldMapper.Builder("child1", INDEX_SETTINGS).ignoreAbove(42))
                    .add(new KeywordFieldMapper.Builder("child2", INDEX_SETTINGS))
            ),
            MapperMergeContext.root(false, false, MAPPING_UPDATE, 1)
        );
        assertEquals(2, root.getTotalFieldsCount());
        assertEquals(2, mergedAdd0.getTotalFieldsCount());
        assertEquals(3, mergedAdd1.getTotalFieldsCount());

        ObjectMapper parent0 = (ObjectMapper) mergedAdd0.getMapper("parent");
        assertNotNull(parent0.getMapper("child1"));
        assertEquals(42, ((KeywordFieldMapper) parent0.getMapper("child1")).fieldType().ignoreAbove().get());
        assertNull(parent0.getMapper("child2"));

        ObjectMapper parent1 = (ObjectMapper) mergedAdd1.getMapper("parent");
        assertNotNull(parent1.getMapper("child1"));
        assertEquals(42, ((KeywordFieldMapper) parent1.getMapper("child1")).fieldType().ignoreAbove().get());
        assertNotNull(parent1.getMapper("child2"));
    }

    public void testMergeWithLimitMultiField() {
        RootObjectMapper.Builder mergeInto = new RootObjectMapper.Builder("_doc").add(createTextKeywordMultiField("text", "keyword1"));
        RootObjectMapper.Builder mergeWith = new RootObjectMapper.Builder("_doc").add(createTextKeywordMultiField("text", "keyword2"));
        RootObjectMapper mergeIntoMapper = mergeInto.build(MapperBuilderContext.root(false, false));
        RootObjectMapper mergeWithMapper = mergeWith.build(MapperBuilderContext.root(false, false));

        assertEquals(2, mergeIntoMapper.getTotalFieldsCount());
        assertEquals(2, mergeWithMapper.getTotalFieldsCount());

        ObjectMapper mergedAdd0 = mergeBuilders(
            new RootObjectMapper.Builder("_doc").add(createTextKeywordMultiField("text", "keyword1")),
            new RootObjectMapper.Builder("_doc").add(createTextKeywordMultiField("text", "keyword2")),
            MapperMergeContext.root(false, false, MAPPING_UPDATE, 0)
        );
        ObjectMapper mergedAdd1 = mergeBuilders(
            new RootObjectMapper.Builder("_doc").add(createTextKeywordMultiField("text", "keyword1")),
            new RootObjectMapper.Builder("_doc").add(createTextKeywordMultiField("text", "keyword2")),
            MapperMergeContext.root(false, false, MAPPING_UPDATE, 1)
        );
        assertEquals(2, mergedAdd0.getTotalFieldsCount());
        assertEquals(3, mergedAdd1.getTotalFieldsCount());
    }

    public void testMergeWithLimitRuntimeField() {
        RootObjectMapper mergeIntoMapper = new RootObjectMapper.Builder("_doc").addRuntimeField(
            new TestRuntimeField("existing_runtime_field", "keyword")
        ).add(createTextKeywordMultiField("text", "keyword1")).build(MapperBuilderContext.root(false, false));
        RootObjectMapper mergeWithMapper = new RootObjectMapper.Builder("_doc").addRuntimeField(
            new TestRuntimeField("existing_runtime_field", "keyword")
        ).addRuntimeField(new TestRuntimeField("new_runtime_field", "keyword")).build(MapperBuilderContext.root(false, false));

        assertEquals(3, mergeIntoMapper.getTotalFieldsCount());
        assertEquals(2, mergeWithMapper.getTotalFieldsCount());

        ObjectMapper mergedAdd0 = mergeBuilders(
            new RootObjectMapper.Builder("_doc").addRuntimeField(new TestRuntimeField("existing_runtime_field", "keyword"))
                .add(createTextKeywordMultiField("text", "keyword1")),
            new RootObjectMapper.Builder("_doc").addRuntimeField(new TestRuntimeField("existing_runtime_field", "keyword"))
                .addRuntimeField(new TestRuntimeField("new_runtime_field", "keyword")),
            MapperMergeContext.root(false, false, MAPPING_UPDATE, 0)
        );
        ObjectMapper mergedAdd1 = mergeBuilders(
            new RootObjectMapper.Builder("_doc").addRuntimeField(new TestRuntimeField("existing_runtime_field", "keyword"))
                .add(createTextKeywordMultiField("text", "keyword1")),
            new RootObjectMapper.Builder("_doc").addRuntimeField(new TestRuntimeField("existing_runtime_field", "keyword"))
                .addRuntimeField(new TestRuntimeField("new_runtime_field", "keyword")),
            MapperMergeContext.root(false, false, MAPPING_UPDATE, 1)
        );
        assertEquals(3, mergedAdd0.getTotalFieldsCount());
        assertEquals(4, mergedAdd1.getTotalFieldsCount());
    }

    public void testMergeSubobjectsFalseWithObject() {
        RootObjectMapper.Builder mergeInto = new RootObjectMapper.Builder("_doc").add(
            new ObjectMapper.Builder("parent", Explicit.of(ObjectMapper.Subobjects.DISABLED))
        );
        RootObjectMapper.Builder mergeWith = new RootObjectMapper.Builder("_doc").add(
            new ObjectMapper.Builder("parent").add(
                new ObjectMapper.Builder("child").add(new KeywordFieldMapper.Builder("grandchild", INDEX_SETTINGS))
            )
        );

        ObjectMapper merged = mergeBuilders(mergeInto, mergeWith, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE));
        ObjectMapper parentMapper = (ObjectMapper) merged.getMapper("parent");
        assertNotNull(parentMapper);
        assertNotNull(parentMapper.getMapper("child.grandchild"));
    }

    public void testConflictingDynamicUpdate() {
        RootObjectMapper.Builder mergeInto = new RootObjectMapper.Builder("_doc").add(
            new KeywordFieldMapper.Builder("http.status_code", INDEX_SETTINGS)
        );
        RootObjectMapper.Builder mergeWith = new RootObjectMapper.Builder("_doc").add(
            new NumberFieldMapper.Builder(
                "http.status_code",
                NumberFieldMapper.NumberType.LONG,
                ScriptCompiler.NONE,
                defaultIndexSettings()
            )
        );

        MapperService.MergeReason autoUpdateMergeReason = randomFrom(MAPPING_AUTO_UPDATE, MAPPING_AUTO_UPDATE_PREFLIGHT);
        ObjectMapper merged = mergeBuilders(
            mergeInto,
            mergeWith,
            MapperMergeContext.root(false, false, autoUpdateMergeReason, Long.MAX_VALUE)
        );
        FieldMapper httpStatusCode = (FieldMapper) merged.getMapper("http.status_code");
        assertThat(httpStatusCode, is(instanceOf(KeywordFieldMapper.class)));

        RootObjectMapper.Builder mergeInto2 = new RootObjectMapper.Builder("_doc").add(
            new KeywordFieldMapper.Builder("http.status_code", INDEX_SETTINGS)
        );
        RootObjectMapper.Builder mergeWith2 = new RootObjectMapper.Builder("_doc").add(
            new NumberFieldMapper.Builder(
                "http.status_code",
                NumberFieldMapper.NumberType.LONG,
                ScriptCompiler.NONE,
                defaultIndexSettings()
            )
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> mergeInto2.mergeWith(mergeWith2, MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE))
        );
        assertThat(e.getMessage(), equalTo("mapper [http.status_code] cannot be changed from type [keyword] to [long]"));
    }

    public void testMergingWithPassThrough() {
        boolean isSourceSynthetic = randomBoolean();
        var objectMapperBuilder = new RootObjectMapper.Builder("_doc").add(
            new ObjectMapper.Builder("metrics").add(new KeywordFieldMapper.Builder("cpu_usage", INDEX_SETTINGS))
        );
        var passThroughMapperBuilder = new RootObjectMapper.Builder("_doc").add(
            new PassThroughObjectMapper.Builder("metrics").setPriority(10)
                .add(new KeywordFieldMapper.Builder("memory_usage", INDEX_SETTINGS))
        );
        RootObjectMapper merged = (RootObjectMapper) mergeBuilders(
            objectMapperBuilder,
            passThroughMapperBuilder,
            MapperMergeContext.root(isSourceSynthetic, true, MAPPING_UPDATE, Long.MAX_VALUE)
        );
        assertThat(merged.getMapper("metrics"), instanceOf(PassThroughObjectMapper.class));
        PassThroughObjectMapper metrics = (PassThroughObjectMapper) merged.getMapper("metrics");
        assertThat(metrics.getMapper("cpu_usage"), instanceOf(KeywordFieldMapper.class));
        assertThat(metrics.getMapper("memory_usage"), instanceOf(KeywordFieldMapper.class));

        var subobjectsTrueBuilder = new RootObjectMapper.Builder("_doc").add(
            new ObjectMapper.Builder("metrics", Explicit.of(ObjectMapper.Subobjects.ENABLED)).add(
                new KeywordFieldMapper.Builder("cpu_usage", INDEX_SETTINGS)
            )
        );
        var passThroughBuilder2 = new RootObjectMapper.Builder("_doc").add(
            new PassThroughObjectMapper.Builder("metrics").setPriority(10)
                .add(new KeywordFieldMapper.Builder("memory_usage", INDEX_SETTINGS))
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> subobjectsTrueBuilder.mergeWith(
                passThroughBuilder2,
                MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE)
            )
        );
        assertThat(
            e.getMessage(),
            equalTo("can't merge a passthrough mapping [metrics] with an object mapping that is either root or has subobjects enabled")
        );
    }

    private RootObjectMapper.Builder createRootSubobjectFalseLeafWithDotsBuilder() {
        FieldMapper.Builder fieldBuilder = new KeywordFieldMapper.Builder("host.name", INDEX_SETTINGS);
        return new RootObjectMapper.Builder("_doc", Explicit.of(ObjectMapper.Subobjects.DISABLED)).add(fieldBuilder);
    }

    private ObjectMapper.Builder createObjectSubobjectsFalseLeafWithDots() {
        KeywordFieldMapper.Builder fieldBuilder = new KeywordFieldMapper.Builder("host.name", INDEX_SETTINGS);
        return new ObjectMapper.Builder("foo", ObjectMapper.Defaults.SUBOBJECTS).add(
            new ObjectMapper.Builder("metrics", Explicit.of(ObjectMapper.Subobjects.DISABLED)).add(fieldBuilder)
        );
    }

    private ObjectMapper.Builder createObjectSubobjectsFalseLeafWithMultiField() {
        TextFieldMapper.Builder fieldBuilder = createTextKeywordMultiField("host.name");
        return new ObjectMapper.Builder("foo", ObjectMapper.Defaults.SUBOBJECTS).add(
            new ObjectMapper.Builder("metrics", Explicit.of(ObjectMapper.Subobjects.DISABLED)).add(fieldBuilder)
        );
    }

    private TextFieldMapper.Builder createTextKeywordMultiField(String name) {
        return createTextKeywordMultiField(name, "keyword");
    }

    private TextFieldMapper.Builder createTextKeywordMultiField(String name, String multiFieldName) {
        TextFieldMapper.Builder builder = new TextFieldMapper.Builder(name, createDefaultIndexAnalyzers());
        builder.multiFieldsBuilder.add(new KeywordFieldMapper.Builder(multiFieldName, INDEX_SETTINGS));
        return builder;
    }
}
