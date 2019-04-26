/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.FieldMapper.CopyTo;
import org.elasticsearch.index.mapper.FieldMapper.MultiFields;
import org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.notNullValue;

public class ObjectMapperMergeTests extends ESTestCase {

    private static FieldMapper barFieldMapper = createTextFieldMapper("bar");
    private static FieldMapper bazFieldMapper = createTextFieldMapper("baz");

    private static RootObjectMapper rootObjectMapper = createMapping(false, true, true, false);

    @AfterClass
    public static void cleanupReferences() {
        barFieldMapper = null;
        bazFieldMapper = null;
        rootObjectMapper = null;
    }

    private static RootObjectMapper createMapping(boolean disabledFieldEnabled, boolean fooFieldEnabled,
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
        assertEquals("Can't update attribute for type [type1.foo.enabled] in index mapping", e.getMessage());
    }

    public void testMergeWhenEnablingField() {
        // GIVEN a mapping with "disabled" field enabled
        ObjectMapper mergeWith = createMapping(true, true, true, false);

        // WHEN merging mappings
        // THEN a MapperException is thrown with an excepted message
        MapperException e = expectThrows(MapperException.class, () -> rootObjectMapper.merge(mergeWith));
        assertEquals("Can't update attribute for type [type1.disabled.enabled] in index mapping", e.getMessage());
    }

    private static RootObjectMapper createRootObjectMapper(String name, boolean enabled, Map<String, Mapper> mappers) {
        final Settings indexSettings = Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT).build();
        final Mapper.BuilderContext context = new Mapper.BuilderContext(indexSettings, new ContentPath());
        final RootObjectMapper rootObjectMapper = new RootObjectMapper.Builder(name).enabled(enabled).build(context);

        mappers.values().forEach(rootObjectMapper::putMapper);

        return rootObjectMapper;
    }

    private static ObjectMapper createObjectMapper(String name, boolean enabled, Map<String, Mapper> mappers) {
        final Settings indexSettings = Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT).build();
        final Mapper.BuilderContext context = new Mapper.BuilderContext(indexSettings, new ContentPath());
        final ObjectMapper mapper = new ObjectMapper.Builder(name).enabled(enabled).build(context);

        mappers.values().forEach(mapper::putMapper);

        return mapper;
    }

    private static TextFieldMapper createTextFieldMapper(String name) {
        final TextFieldType fieldType = new TextFieldType();
        final Settings indexSettings = Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT).build();

        return new TextFieldMapper(name, fieldType, fieldType, -1, null, indexSettings, MultiFields.empty(), CopyTo.empty());
    }
}
