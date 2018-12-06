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

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

public class MapperMergeValidatorTests extends ESTestCase {

    public void testDuplicateFieldAliasAndObject() {
        ObjectMapper objectMapper = createObjectMapper("some.path");
        FieldAliasMapper aliasMapper = new FieldAliasMapper("path", "some.path", "field");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            MapperMergeValidator.validateMapperStructure("type",
                singletonList(objectMapper),
                emptyList(),
                singletonList(aliasMapper),
                emptyMap(),
                new FieldTypeLookup(),
                true));
        assertEquals("Field [some.path] is defined both as an object and a field in [type]", e.getMessage());
    }

    public void testFieldAliasWithNestedScope() {
        ObjectMapper objectMapper = createNestedObjectMapper("nested");
        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "nested.alias", "nested.field");

        MapperMergeValidator.validateFieldReferences(createIndexSettings(),
            emptyList(),
            singletonList(aliasMapper),
            Collections.singletonMap("nested", objectMapper),
            new FieldTypeLookup());
    }

    public void testFieldAliasWithDifferentObjectScopes() {
        Map<String, ObjectMapper> fullPathObjectMappers = new HashMap<>();
        fullPathObjectMappers.put("object1", createObjectMapper("object1"));
        fullPathObjectMappers.put("object2", createObjectMapper("object2"));

        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "object2.alias", "object1.field");

        MapperMergeValidator.validateFieldReferences(createIndexSettings(),
            emptyList(),
            singletonList(aliasMapper),
            fullPathObjectMappers,
            new FieldTypeLookup());
    }

    public void testFieldAliasWithNestedTarget() {
        ObjectMapper objectMapper = createNestedObjectMapper("nested");
        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "alias", "nested.field");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            MapperMergeValidator.validateFieldReferences(createIndexSettings(),
                emptyList(),
                singletonList(aliasMapper),
                Collections.singletonMap("nested", objectMapper),
                new FieldTypeLookup()));

        String expectedMessage = "Invalid [path] value [nested.field] for field alias [alias]: " +
            "an alias must have the same nested scope as its target. The alias is not nested, " +
            "but the target's nested scope is [nested].";
        assertEquals(expectedMessage, e.getMessage());
    }

    public void testFieldAliasWithDifferentNestedScopes() {
        Map<String, ObjectMapper> fullPathObjectMappers = new HashMap<>();
        fullPathObjectMappers.put("nested1", createNestedObjectMapper("nested1"));
        fullPathObjectMappers.put("nested2", createNestedObjectMapper("nested2"));

        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "nested2.alias", "nested1.field");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            MapperMergeValidator.validateFieldReferences(createIndexSettings(),
                emptyList(),
                singletonList(aliasMapper),
                fullPathObjectMappers,
                new FieldTypeLookup()));


        String expectedMessage = "Invalid [path] value [nested1.field] for field alias [nested2.alias]: " +
            "an alias must have the same nested scope as its target. The alias's nested scope is [nested2], " +
            "but the target's nested scope is [nested1].";
        assertEquals(expectedMessage, e.getMessage());
    }

    private static ObjectMapper createObjectMapper(String name) {
        return new ObjectMapper(name, name, true,
            ObjectMapper.Nested.NO,
            ObjectMapper.Dynamic.FALSE, false, emptyMap(), createSettings());
    }

    private static ObjectMapper createNestedObjectMapper(String name) {
        return new ObjectMapper(name, name, true,
            ObjectMapper.Nested.newNested(false, false),
            ObjectMapper.Dynamic.FALSE, false, emptyMap(), createSettings());
    }

    private static IndexSettings createIndexSettings() {
        return new IndexSettings(
            IndexMetaData.builder("index").settings(createSettings())
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            Settings.EMPTY);
    }

    private static Settings createSettings() {
        return Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
            .build();
    }
}
