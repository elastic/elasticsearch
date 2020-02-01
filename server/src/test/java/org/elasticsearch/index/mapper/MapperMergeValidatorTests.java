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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

public class MapperMergeValidatorTests extends ESTestCase {

    public void testMismatchedFieldTypes() {
        FieldMapper existingField = new MockFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup()
            .copyAndAddAll("type", singletonList(existingField), emptyList());

        FieldTypeLookupTests.OtherFakeFieldType newFieldType = new FieldTypeLookupTests.OtherFakeFieldType();
        newFieldType.setName("foo");
        FieldMapper invalidField = new MockFieldMapper("foo", newFieldType);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            MapperMergeValidator.validateNewMappers(
                emptyList(),
                singletonList(invalidField),
                emptyList(),
                lookup));
        assertTrue(e.getMessage().contains("cannot be changed from type [faketype] to [otherfaketype]"));
    }

    public void testConflictingFieldTypes() {
        FieldMapper existingField = new MockFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup()
            .copyAndAddAll("type", singletonList(existingField), emptyList());

        MappedFieldType newFieldType = new MockFieldMapper.FakeFieldType();
        newFieldType.setName("foo");
        newFieldType.setBoost(2.0f);
        FieldMapper validField = new MockFieldMapper("foo", newFieldType);

        // Boost is updateable, so no exception should be thrown.
        MapperMergeValidator.validateNewMappers(
            emptyList(),
            singletonList(validField),
            emptyList(),
            lookup);

        MappedFieldType invalidFieldType = new MockFieldMapper.FakeFieldType();
        invalidFieldType.setName("foo");
        invalidFieldType.setStored(true);
        FieldMapper invalidField = new MockFieldMapper("foo", invalidFieldType);

        // Store is not updateable, so we expect an exception.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            MapperMergeValidator.validateNewMappers(
                emptyList(),
                singletonList(invalidField),
                emptyList(),
                lookup));
        assertTrue(e.getMessage().contains("has different [store] values"));
    }

    public void testDuplicateFieldAliasAndObject() {
        ObjectMapper objectMapper = createObjectMapper("some.path");
        FieldAliasMapper aliasMapper = new FieldAliasMapper("path", "some.path", "field");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            MapperMergeValidator.validateNewMappers(
                singletonList(objectMapper),
                emptyList(),
                singletonList(aliasMapper),
                new FieldTypeLookup()));
        assertEquals("Field [some.path] is defined both as an object and a field.", e.getMessage());
    }

    public void testDuplicateFieldAliasAndConcreteField() {
        FieldMapper field = new MockFieldMapper("field");
        FieldMapper invalidField = new MockFieldMapper("invalid");
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid", "invalid", "field");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            MapperMergeValidator.validateNewMappers(
                emptyList(),
                Arrays.asList(field, invalidField),
                singletonList(invalidAlias),
                new FieldTypeLookup()));

        assertEquals("Field [invalid] is defined both as an alias and a concrete field.", e.getMessage());
    }

    public void testAliasThatRefersToAlias() {
        FieldMapper field = new MockFieldMapper("field");
        FieldAliasMapper alias = new FieldAliasMapper("alias", "alias", "field");
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid-alias", "invalid-alias", "alias");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            MapperMergeValidator.validateNewMappers(
                emptyList(),
                singletonList(field),
                Arrays.asList(alias, invalidAlias),
                new FieldTypeLookup()));

        assertEquals("Invalid [path] value [alias] for field alias [invalid-alias]: an alias" +
            " cannot refer to another alias.", e.getMessage());
    }

    public void testAliasThatRefersToItself() {
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid-alias", "invalid-alias", "invalid-alias");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            MapperMergeValidator.validateNewMappers(
                emptyList(),
                emptyList(),
                singletonList(invalidAlias),
                new FieldTypeLookup()));

        assertEquals("Invalid [path] value [invalid-alias] for field alias [invalid-alias]: an alias" +
            " cannot refer to itself.", e.getMessage());
    }

    public void testAliasWithNonExistentPath() {
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid-alias", "invalid-alias", "non-existent");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            MapperMergeValidator.validateNewMappers(
                emptyList(),
                emptyList(),
                singletonList(invalidAlias),
                new FieldTypeLookup()));

        assertEquals("Invalid [path] value [non-existent] for field alias [invalid-alias]: an alias" +
            " must refer to an existing field in the mappings.", e.getMessage());
    }

    public void testFieldAliasWithNestedScope() {
        ObjectMapper objectMapper = createNestedObjectMapper("nested");
        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "nested.alias", "nested.field");

        MapperMergeValidator.validateFieldReferences(emptyList(),
            singletonList(aliasMapper),
            Collections.singletonMap("nested", objectMapper),
            new FieldTypeLookup());
    }

    public void testFieldAliasWithDifferentObjectScopes() {
        Map<String, ObjectMapper> fullPathObjectMappers = new HashMap<>();
        fullPathObjectMappers.put("object1", createObjectMapper("object1"));
        fullPathObjectMappers.put("object2", createObjectMapper("object2"));

        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "object2.alias", "object1.field");

        MapperMergeValidator.validateFieldReferences(emptyList(),
            singletonList(aliasMapper),
            fullPathObjectMappers,
            new FieldTypeLookup());
    }

    public void testFieldAliasWithNestedTarget() {
        ObjectMapper objectMapper = createNestedObjectMapper("nested");
        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "alias", "nested.field");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            MapperMergeValidator.validateFieldReferences(emptyList(),
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
            MapperMergeValidator.validateFieldReferences(emptyList(),
                singletonList(aliasMapper),
                fullPathObjectMappers,
                new FieldTypeLookup()));


        String expectedMessage = "Invalid [path] value [nested1.field] for field alias [nested2.alias]: " +
            "an alias must have the same nested scope as its target. The alias's nested scope is [nested2], " +
            "but the target's nested scope is [nested1].";
        assertEquals(expectedMessage, e.getMessage());
    }

    private static final Settings SETTINGS = Settings.builder()
        .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
        .build();

    private static ObjectMapper createObjectMapper(String name) {
        return new ObjectMapper(name, name, true,
            ObjectMapper.Nested.NO,
            ObjectMapper.Dynamic.FALSE, emptyMap(), SETTINGS);
    }

    private static ObjectMapper createNestedObjectMapper(String name) {
        return new ObjectMapper(name, name, true,
            ObjectMapper.Nested.newNested(false, false),
            ObjectMapper.Dynamic.FALSE, emptyMap(), SETTINGS);
    }
}
