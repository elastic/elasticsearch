/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class ObjectMapperMergeTests extends ESTestCase {

    private final FieldMapper barFieldMapper = createTextFieldMapper("bar");
    private final FieldMapper bazFieldMapper = createTextFieldMapper("baz");

    private final RootObjectMapper rootObjectMapper = createMapping(false, true, true, false);

    private RootObjectMapper createMapping(
        boolean disabledFieldEnabled,
        boolean fooFieldEnabled,
        boolean includeBarField,
        boolean includeBazField
    ) {
        Map<String, Mapper> mappers = new HashMap<>();
        mappers.put("disabled", createObjectMapper("disabled", disabledFieldEnabled, emptyMap()));
        Map<String, Mapper> fooMappers = new HashMap<>();
        if (includeBarField) {
            fooMappers.put("bar", barFieldMapper);
        }
        if (includeBazField) {
            fooMappers.put("baz", bazFieldMapper);
        }
        mappers.put("foo", createObjectMapper("foo", fooFieldEnabled, Collections.unmodifiableMap(fooMappers)));
        return createRootObjectMapper("type1", true, Collections.unmodifiableMap(mappers));
    }

    public void testMerge() {
        // GIVEN an enriched mapping with "baz" new field
        ObjectMapper mergeWith = createMapping(false, true, true, true);

        // WHEN merging mappings
        final ObjectMapper merged = rootObjectMapper.merge(mergeWith, MapperBuilderContext.ROOT);

        // THEN "baz" new field is added to merged mapping
        final ObjectMapper mergedFoo = (ObjectMapper) merged.getMapper("foo");
        assertThat(mergedFoo.getMapper("bar"), notNullValue());
        assertThat(mergedFoo.getMapper("baz"), notNullValue());
    }

    public void testMergeWhenDisablingField() {
        // GIVEN a mapping with "foo" field disabled
        ObjectMapper mergeWith = createMapping(false, false, false, false);

        // WHEN merging mappings
        // THEN a MapperException is thrown with an excepted message
        MapperException e = expectThrows(MapperException.class, () -> rootObjectMapper.merge(mergeWith, MapperBuilderContext.ROOT));
        assertEquals("the [enabled] parameter can't be updated for the object mapping [foo]", e.getMessage());
    }

    public void testMergeDisabledField() {
        // GIVEN a mapping with "foo" field disabled
        Map<String, Mapper> mappers = new HashMap<>();
        // the field is disabled, and we are not trying to re-enable it, hence merge should work
        mappers.put("disabled", new ObjectMapper.Builder("disabled").build(MapperBuilderContext.ROOT));
        RootObjectMapper mergeWith = createRootObjectMapper("type1", true, Collections.unmodifiableMap(mappers));

        RootObjectMapper merged = (RootObjectMapper) rootObjectMapper.merge(mergeWith, MapperBuilderContext.ROOT);
        assertFalse(((ObjectMapper) merged.getMapper("disabled")).isEnabled());
    }

    public void testMergeEnabled() {
        ObjectMapper mergeWith = createMapping(true, true, true, false);

        MapperException e = expectThrows(MapperException.class, () -> rootObjectMapper.merge(mergeWith, MapperBuilderContext.ROOT));
        assertEquals("the [enabled] parameter can't be updated for the object mapping [disabled]", e.getMessage());

        ObjectMapper result = rootObjectMapper.merge(mergeWith, MapperService.MergeReason.INDEX_TEMPLATE, MapperBuilderContext.ROOT);
        assertTrue(result.isEnabled());
    }

    public void testMergeEnabledForRootMapper() {
        String type = MapperService.SINGLE_MAPPING_NAME;
        ObjectMapper firstMapper = createRootObjectMapper(type, true, Collections.emptyMap());
        ObjectMapper secondMapper = createRootObjectMapper(type, false, Collections.emptyMap());

        MapperException e = expectThrows(MapperException.class, () -> firstMapper.merge(secondMapper, MapperBuilderContext.ROOT));
        assertEquals("the [enabled] parameter can't be updated for the object mapping [" + type + "]", e.getMessage());

        ObjectMapper result = firstMapper.merge(secondMapper, MapperService.MergeReason.INDEX_TEMPLATE, MapperBuilderContext.ROOT);
        assertFalse(result.isEnabled());
    }

    public void testMergeDisabledRootMapper() {
        String type = MapperService.SINGLE_MAPPING_NAME;
        final RootObjectMapper rootObjectMapper = (RootObjectMapper) new RootObjectMapper.Builder(type).enabled(false)
            .build(MapperBuilderContext.ROOT);
        // the root is disabled, and we are not trying to re-enable it, but we do want to be able to add runtime fields
        final RootObjectMapper mergeWith = new RootObjectMapper.Builder(type).addRuntimeFields(
            Collections.singletonMap("test", new TestRuntimeField("test", "long"))
        ).build(MapperBuilderContext.ROOT);

        RootObjectMapper merged = (RootObjectMapper) rootObjectMapper.merge(mergeWith, MapperBuilderContext.ROOT);
        assertFalse(merged.isEnabled());
        assertEquals(1, merged.runtimeFields().size());
        assertEquals("test", merged.runtimeFields().iterator().next().name());
    }

    public void testMergeNested() {
        NestedObjectMapper firstMapper = new NestedObjectMapper.Builder("nested1", Version.CURRENT).includeInParent(true)
            .includeInRoot(true)
            .build(MapperBuilderContext.ROOT);
        NestedObjectMapper secondMapper = new NestedObjectMapper.Builder("nested1", Version.CURRENT).includeInParent(false)
            .includeInRoot(true)
            .build(MapperBuilderContext.ROOT);

        MapperException e = expectThrows(MapperException.class, () -> firstMapper.merge(secondMapper, MapperBuilderContext.ROOT));
        assertThat(e.getMessage(), containsString("[include_in_parent] parameter can't be updated on a nested object mapping"));

        NestedObjectMapper result = (NestedObjectMapper) firstMapper.merge(
            secondMapper,
            MapperService.MergeReason.INDEX_TEMPLATE,
            MapperBuilderContext.ROOT
        );
        assertFalse(result.isIncludeInParent());
        assertTrue(result.isIncludeInRoot());
    }

    public void testMergedFieldNamesFieldWithDotsSubobjectsFalseAtRoot() {
        RootObjectMapper mergeInto = createRootSubobjectFalseLeafWithDots();
        RootObjectMapper mergeWith = createRootSubobjectFalseLeafWithDots();

        final ObjectMapper merged = mergeInto.merge(mergeWith, MapperBuilderContext.ROOT);

        final KeywordFieldMapper keywordFieldMapper = (KeywordFieldMapper) merged.getMapper("host.name");
        assertEquals("host.name", keywordFieldMapper.name());
        assertEquals("host.name", keywordFieldMapper.simpleName());
    }

    public void testMergedFieldNamesFieldWithDotsSubobjectsFalse() {
        RootObjectMapper mergeInto = createRootObjectMapper(
            "_doc",
            true,
            Collections.singletonMap("foo", createObjectSubobjectsFalseLeafWithDots())
        );
        RootObjectMapper mergeWith = createRootObjectMapper(
            "_doc",
            true,
            Collections.singletonMap("foo", createObjectSubobjectsFalseLeafWithDots())
        );

        final ObjectMapper merged = mergeInto.merge(mergeWith, MapperBuilderContext.ROOT);

        ObjectMapper foo = (ObjectMapper) merged.getMapper("foo");
        ObjectMapper metrics = (ObjectMapper) foo.getMapper("metrics");
        final KeywordFieldMapper keywordFieldMapper = (KeywordFieldMapper) metrics.getMapper("host.name");
        assertEquals("foo.metrics.host.name", keywordFieldMapper.name());
        assertEquals("host.name", keywordFieldMapper.simpleName());
    }

    public void testMergedFieldNamesMultiFields() {
        RootObjectMapper mergeInto = createRootObjectMapper(
            "_doc",
            true,
            Collections.singletonMap("text", createTextKeywordMultiField("text", MapperBuilderContext.ROOT))
        );
        RootObjectMapper mergeWith = createRootObjectMapper(
            "_doc",
            true,
            Collections.singletonMap("text", createTextKeywordMultiField("text", MapperBuilderContext.ROOT))
        );

        final ObjectMapper merged = mergeInto.merge(mergeWith, MapperBuilderContext.ROOT);

        TextFieldMapper text = (TextFieldMapper) merged.getMapper("text");
        assertEquals("text", text.name());
        assertEquals("text", text.simpleName());
        KeywordFieldMapper keyword = (KeywordFieldMapper) text.multiFields().iterator().next();
        assertEquals("text.keyword", keyword.name());
        assertEquals("keyword", keyword.simpleName());
    }

    public void testMergedFieldNamesMultiFieldsWithinSubobjectsFalse() {
        RootObjectMapper mergeInto = createRootObjectMapper(
            "_doc",
            true,
            Collections.singletonMap("foo", createObjectSubobjectsFalseLeafWithMultiField())
        );
        RootObjectMapper mergeWith = createRootObjectMapper(
            "_doc",
            true,
            Collections.singletonMap("foo", createObjectSubobjectsFalseLeafWithMultiField())
        );

        final ObjectMapper merged = mergeInto.merge(mergeWith, MapperBuilderContext.ROOT);

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
        FieldMapper fieldMapper = new KeywordFieldMapper.Builder("host.name", Version.CURRENT).build(MapperBuilderContext.ROOT);
        assertEquals("host.name", fieldMapper.simpleName());
        assertEquals("host.name", fieldMapper.name());
        return (RootObjectMapper) new RootObjectMapper.Builder("_doc").subobjects(false)
            .addMappers(Collections.singletonMap("host.name", fieldMapper))
            .build(MapperBuilderContext.ROOT);
    }

    private static RootObjectMapper createRootObjectMapper(String name, boolean enabled, Map<String, Mapper> mappers) {
        return (RootObjectMapper) new RootObjectMapper.Builder(name).enabled(enabled).addMappers(mappers).build(MapperBuilderContext.ROOT);
    }

    private static ObjectMapper createObjectMapper(String name, boolean enabled, Map<String, Mapper> mappers) {
        return new ObjectMapper.Builder(name).enabled(enabled).addMappers(mappers).build(MapperBuilderContext.ROOT);
    }

    private static ObjectMapper createObjectSubobjectsFalseLeafWithDots() {
        KeywordFieldMapper fieldMapper = new KeywordFieldMapper.Builder("host.name", Version.CURRENT).build(
            new MapperBuilderContext("foo.metrics")
        );
        assertEquals("host.name", fieldMapper.simpleName());
        assertEquals("foo.metrics.host.name", fieldMapper.name());
        ObjectMapper metrics = new ObjectMapper.Builder("metrics").subobjects(false)
            .addMappers(Collections.singletonMap("host.name", fieldMapper))
            .build(new MapperBuilderContext("foo"));
        return new ObjectMapper.Builder("foo").addMappers(Collections.singletonMap("metrics", metrics)).build(MapperBuilderContext.ROOT);
    }

    private ObjectMapper createObjectSubobjectsFalseLeafWithMultiField() {
        TextFieldMapper textKeywordMultiField = createTextKeywordMultiField("host.name", new MapperBuilderContext("foo.metrics"));
        assertEquals("host.name", textKeywordMultiField.simpleName());
        assertEquals("foo.metrics.host.name", textKeywordMultiField.name());
        FieldMapper fieldMapper = textKeywordMultiField.multiFields.iterator().next();
        assertEquals("keyword", fieldMapper.simpleName());
        assertEquals("foo.metrics.host.name.keyword", fieldMapper.name());
        ObjectMapper metrics = new ObjectMapper.Builder("metrics").subobjects(false)
            .addMappers(Collections.singletonMap("host.name", textKeywordMultiField))
            .build(new MapperBuilderContext("foo"));
        return new ObjectMapper.Builder("foo").addMappers(Collections.singletonMap("metrics", metrics)).build(MapperBuilderContext.ROOT);
    }

    private TextFieldMapper createTextFieldMapper(String name) {
        return new TextFieldMapper.Builder(name, createDefaultIndexAnalyzers()).build(MapperBuilderContext.ROOT);
    }

    private TextFieldMapper createTextKeywordMultiField(String name, MapperBuilderContext mapperBuilderContext) {
        TextFieldMapper.Builder builder = new TextFieldMapper.Builder(name, createDefaultIndexAnalyzers());
        builder.multiFieldsBuilder.add(new KeywordFieldMapper.Builder("keyword", Version.CURRENT));
        return builder.build(mapperBuilderContext);
    }
}
