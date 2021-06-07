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

    private RootObjectMapper createMapping(boolean disabledFieldEnabled, boolean fooFieldEnabled,
                                                  boolean includeBarField, boolean includeBazField) {
        Map<String, Mapper> mappers = new HashMap<>();
        mappers.put("disabled", createObjectMapper("disabled", disabledFieldEnabled, emptyMap()));
        Map<String, Mapper> fooMappers = new HashMap<>();
        if (includeBarField) {
            fooMappers.put("bar", barFieldMapper);
        }
        if (includeBazField) {
            fooMappers.put("baz", bazFieldMapper);
        }
        mappers.put("foo", createObjectMapper("foo", fooFieldEnabled,  Collections.unmodifiableMap(fooMappers)));
        return createRootObjectMapper("type1", true, Collections.unmodifiableMap(mappers));
    }

    public void testMerge() {
        // GIVEN an enriched mapping with "baz" new field
        ObjectMapper mergeWith = createMapping(false, true, true, true);

        // WHEN merging mappings
        final ObjectMapper merged = rootObjectMapper.merge(mergeWith);

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
        MapperException e = expectThrows(MapperException.class, () -> rootObjectMapper.merge(mergeWith));
        assertEquals("the [enabled] parameter can't be updated for the object mapping [foo]", e.getMessage());
    }

    public void testMergeDisabledField() {
        // GIVEN a mapping with "foo" field disabled
        Map<String, Mapper> mappers = new HashMap<>();
        //the field is disabled, and we are not trying to re-enable it, hence merge should work
        mappers.put("disabled", new ObjectMapper.Builder("disabled").build(new ContentPath()));
        RootObjectMapper mergeWith = createRootObjectMapper("type1", true, Collections.unmodifiableMap(mappers));

        RootObjectMapper merged = (RootObjectMapper)rootObjectMapper.merge(mergeWith);
        assertFalse(((ObjectMapper)merged.getMapper("disabled")).isEnabled());
    }

    public void testMergeEnabled() {
        ObjectMapper mergeWith = createMapping(true, true, true, false);

        MapperException e = expectThrows(MapperException.class, () -> rootObjectMapper.merge(mergeWith));
        assertEquals("the [enabled] parameter can't be updated for the object mapping [disabled]", e.getMessage());

        ObjectMapper result = rootObjectMapper.merge(mergeWith, MapperService.MergeReason.INDEX_TEMPLATE);
        assertTrue(result.isEnabled());
    }

    public void testMergeEnabledForRootMapper() {
        String type = MapperService.SINGLE_MAPPING_NAME;
        ObjectMapper firstMapper = createRootObjectMapper(type, true, Collections.emptyMap());
        ObjectMapper secondMapper = createRootObjectMapper(type, false, Collections.emptyMap());

        MapperException e = expectThrows(MapperException.class, () -> firstMapper.merge(secondMapper));
        assertEquals("the [enabled] parameter can't be updated for the object mapping [" + type + "]", e.getMessage());

        ObjectMapper result = firstMapper.merge(secondMapper, MapperService.MergeReason.INDEX_TEMPLATE);
        assertFalse(result.isEnabled());
    }

    public void testMergeDisabledRootMapper() {
        String type = MapperService.SINGLE_MAPPING_NAME;
        final RootObjectMapper rootObjectMapper =
            (RootObjectMapper) new RootObjectMapper.Builder(type).enabled(false).build(new ContentPath());
        //the root is disabled, and we are not trying to re-enable it, but we do want to be able to add runtime fields
        final RootObjectMapper mergeWith =
            new RootObjectMapper.Builder(type).setRuntime(
                Collections.singletonMap("test", new TestRuntimeField("test", "long"))).build(new ContentPath());

        RootObjectMapper merged = (RootObjectMapper) rootObjectMapper.merge(mergeWith);
        assertFalse(merged.isEnabled());
        assertEquals(1, merged.runtimeFields().size());
        assertEquals("test", merged.runtimeFields().iterator().next().name());
    }

    public void testMergeNested() {

        NestedObjectMapper firstMapper = new NestedObjectMapper.Builder("nested1", Version.CURRENT)
            .includeInParent(true)
            .includeInRoot(true)
            .build(new ContentPath());
        NestedObjectMapper secondMapper = new NestedObjectMapper.Builder("nested1", Version.CURRENT)
            .includeInParent(false)
            .includeInRoot(true)
            .build(new ContentPath());

        MapperException e = expectThrows(MapperException.class, () -> firstMapper.merge(secondMapper));
        assertThat(e.getMessage(), containsString("[include_in_parent] parameter can't be updated on a nested object mapping"));

        NestedObjectMapper result = (NestedObjectMapper) firstMapper.merge(secondMapper, MapperService.MergeReason.INDEX_TEMPLATE);
        assertFalse(result.isIncludeInParent());
        assertTrue(result.isIncludeInRoot());
    }

    private static RootObjectMapper createRootObjectMapper(String name, boolean enabled, Map<String, Mapper> mappers) {
        final RootObjectMapper rootObjectMapper
            = (RootObjectMapper) new RootObjectMapper.Builder(name).enabled(enabled).build(new ContentPath());

        mappers.values().forEach(rootObjectMapper::putMapper);

        return rootObjectMapper;
    }

    private static ObjectMapper createObjectMapper(String name, boolean enabled, Map<String, Mapper> mappers) {
        final ObjectMapper mapper = new ObjectMapper.Builder(name).enabled(enabled).build(new ContentPath());

        mappers.values().forEach(mapper::putMapper);

        return mapper;
    }

    private TextFieldMapper createTextFieldMapper(String name) {
        return new TextFieldMapper.Builder(name, createDefaultIndexAnalyzers()).build(new ContentPath());
    }
}
