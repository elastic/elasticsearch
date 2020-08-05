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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

public class FieldAliasMapperValidationTests extends ESTestCase {

    public void testDuplicateFieldAliasAndObject() {
        ObjectMapper objectMapper = createObjectMapper("some.path");
        FieldAliasMapper aliasMapper = new FieldAliasMapper("path", "some.path", "field");

        MapperParsingException e = expectThrows(MapperParsingException.class, () ->
            new MappingLookup(
                Collections.emptyList(),
                singletonList(objectMapper),
                singletonList(aliasMapper), 0, Lucene.STANDARD_ANALYZER));
        assertEquals("Alias [some.path] is defined both as an object and an alias", e.getMessage());
    }

    public void testDuplicateFieldAliasAndConcreteField() {
        FieldMapper field = new MockFieldMapper("field");
        FieldMapper invalidField = new MockFieldMapper("invalid");
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid", "invalid", "field");

        MapperParsingException e = expectThrows(MapperParsingException.class, () ->
            new MappingLookup(
                Arrays.asList(field, invalidField),
                emptyList(),
                singletonList(invalidAlias), 0, Lucene.STANDARD_ANALYZER));

        assertEquals("Alias [invalid] is defined both as an alias and a concrete field", e.getMessage());
    }

    public void testAliasThatRefersToAlias() {
        FieldMapper field = new MockFieldMapper("field");
        FieldAliasMapper alias = new FieldAliasMapper("alias", "alias", "field");
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid-alias", "invalid-alias", "alias");

        MappingLookup mappers = new MappingLookup(
            singletonList(field),
            emptyList(),
            Arrays.asList(alias, invalidAlias), 0, Lucene.STANDARD_ANALYZER);
        alias.validate(mappers);

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            invalidAlias.validate(mappers);
        });

        assertEquals("Invalid [path] value [alias] for field alias [invalid-alias]: an alias" +
            " cannot refer to another alias.", e.getMessage());
    }

    public void testAliasThatRefersToItself() {
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid-alias", "invalid-alias", "invalid-alias");

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            MappingLookup mappers = new MappingLookup(
                emptyList(),
                emptyList(),
                singletonList(invalidAlias), 0, null);
            invalidAlias.validate(mappers);
        });

        assertEquals("Invalid [path] value [invalid-alias] for field alias [invalid-alias]: an alias" +
            " cannot refer to itself.", e.getMessage());
    }

    public void testAliasWithNonExistentPath() {
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid-alias", "invalid-alias", "non-existent");

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            MappingLookup mappers = new MappingLookup(
                emptyList(),
                emptyList(),
                singletonList(invalidAlias), 0, Lucene.STANDARD_ANALYZER);
            invalidAlias.validate(mappers);
        });

        assertEquals("Invalid [path] value [non-existent] for field alias [invalid-alias]: an alias" +
            " must refer to an existing field in the mappings.", e.getMessage());
    }

    public void testFieldAliasWithNestedScope() {
        ObjectMapper objectMapper = createNestedObjectMapper("nested");
        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "nested.alias", "nested.field");

        MappingLookup mappers = new MappingLookup(
            singletonList(createFieldMapper("nested", "field")),
            singletonList(objectMapper),
            singletonList(aliasMapper),
            0, Lucene.STANDARD_ANALYZER);
        aliasMapper.validate(mappers);
    }

    public void testFieldAliasWithDifferentObjectScopes() {

        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "object2.alias", "object1.field");

        MappingLookup mappers = new MappingLookup(
            List.of(createFieldMapper("object1", "field")),
            List.of(createObjectMapper("object1"), createObjectMapper("object2")),
            singletonList(aliasMapper),
            0, Lucene.STANDARD_ANALYZER);
        aliasMapper.validate(mappers);
    }

    public void testFieldAliasWithNestedTarget() {
        ObjectMapper objectMapper = createNestedObjectMapper("nested");
        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "alias", "nested.field");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            MappingLookup mappers = new MappingLookup(
                singletonList(createFieldMapper("nested", "field")),
                Collections.singletonList(objectMapper),
                singletonList(aliasMapper),
                0, Lucene.STANDARD_ANALYZER);
            aliasMapper.validate(mappers);
        });

        String expectedMessage = "Invalid [path] value [nested.field] for field alias [alias]: " +
            "an alias must have the same nested scope as its target. The alias is not nested, " +
            "but the target's nested scope is [nested].";
        assertEquals(expectedMessage, e.getMessage());
    }

    public void testFieldAliasWithDifferentNestedScopes() {
        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "nested2.alias", "nested1.field");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            MappingLookup mappers = new MappingLookup(
                singletonList(createFieldMapper("nested1", "field")),
                List.of(createNestedObjectMapper("nested1"), createNestedObjectMapper("nested2")),
                singletonList(aliasMapper),
                0, Lucene.STANDARD_ANALYZER);
            aliasMapper.validate(mappers);
        });


        String expectedMessage = "Invalid [path] value [nested1.field] for field alias [nested2.alias]: " +
            "an alias must have the same nested scope as its target. The alias's nested scope is [nested2], " +
            "but the target's nested scope is [nested1].";
        assertEquals(expectedMessage, e.getMessage());
    }

    private static final Settings SETTINGS = Settings.builder()
        .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
        .build();

    private static FieldMapper createFieldMapper(String parent, String name) {
        Mapper.BuilderContext context = new Mapper.BuilderContext(SETTINGS, new ContentPath(parent));
        return new BooleanFieldMapper.Builder(name).build(context);
    }

    private static ObjectMapper createObjectMapper(String name) {
        return new ObjectMapper(name, name,
            new Explicit<>(true, false),
            ObjectMapper.Nested.NO,
            ObjectMapper.Dynamic.FALSE, emptyMap(), SETTINGS);
    }

    private static ObjectMapper createNestedObjectMapper(String name) {
        return new ObjectMapper(name, name,
            new Explicit<>(true, false),
            ObjectMapper.Nested.newNested(),
            ObjectMapper.Dynamic.FALSE, emptyMap(), SETTINGS);
    }
}
