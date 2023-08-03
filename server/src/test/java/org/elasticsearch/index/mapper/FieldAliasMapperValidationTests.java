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
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

public class FieldAliasMapperValidationTests extends ESTestCase {

    public void testDuplicateFieldAliasAndObject() {
        ObjectMapper objectMapper = createObjectMapper("some.path");
        FieldAliasMapper aliasMapper = new FieldAliasMapper("path", "some.path", "field");

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createMappingLookup(emptyList(), singletonList(objectMapper), singletonList(aliasMapper), emptyList())
        );
        assertEquals("Alias [some.path] is defined both as an object and an alias", e.getMessage());
    }

    public void testDuplicateFieldAliasAndConcreteField() {
        FieldMapper field = new MockFieldMapper("field");
        FieldMapper invalidField = new MockFieldMapper("invalid");
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid", "invalid", "field");

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createMappingLookup(Arrays.asList(field, invalidField), emptyList(), singletonList(invalidAlias), emptyList())
        );
        assertEquals("Alias [invalid] is defined both as an alias and a concrete field", e.getMessage());
    }

    public void testAliasThatRefersToAlias() {
        FieldMapper field = new MockFieldMapper("field");
        FieldAliasMapper alias = new FieldAliasMapper("alias", "alias", "field");
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid-alias", "invalid-alias", "alias");

        MappingLookup mappers = createMappingLookup(singletonList(field), emptyList(), Arrays.asList(alias, invalidAlias), emptyList());
        alias.validate(mappers);

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> { invalidAlias.validate(mappers); });

        assertEquals(
            "Invalid [path] value [alias] for field alias [invalid-alias]: an alias" + " cannot refer to another alias.",
            e.getMessage()
        );
    }

    public void testAliasThatRefersToItself() {
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid-alias", "invalid-alias", "invalid-alias");

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            MappingLookup mappers = createMappingLookup(emptyList(), emptyList(), singletonList(invalidAlias), emptyList());
            invalidAlias.validate(mappers);
        });

        assertEquals(
            "Invalid [path] value [invalid-alias] for field alias [invalid-alias]: an alias" + " cannot refer to itself.",
            e.getMessage()
        );
    }

    public void testAliasWithNonExistentPath() {
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid-alias", "invalid-alias", "non-existent");

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            MappingLookup mappers = createMappingLookup(emptyList(), emptyList(), singletonList(invalidAlias), emptyList());
            invalidAlias.validate(mappers);
        });

        assertEquals(
            "Invalid [path] value [non-existent] for field alias [invalid-alias]: an alias"
                + " must refer to an existing field in the mappings.",
            e.getMessage()
        );
    }

    public void testFieldAliasWithNestedScope() {
        ObjectMapper objectMapper = createNestedObjectMapper("nested");
        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "nested.alias", "nested.field");

        MappingLookup mappers = createMappingLookup(
            singletonList(createFieldMapper("nested", "field")),
            singletonList(objectMapper),
            singletonList(aliasMapper),
            emptyList()
        );
        aliasMapper.validate(mappers);
    }

    public void testFieldAliasWithDifferentObjectScopes() {

        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "object2.alias", "object1.field");

        MappingLookup mappers = createMappingLookup(
            List.of(createFieldMapper("object1", "field")),
            List.of(createObjectMapper("object1"), createObjectMapper("object2")),
            singletonList(aliasMapper),
            emptyList()
        );
        aliasMapper.validate(mappers);
    }

    public void testFieldAliasWithNestedTarget() {
        ObjectMapper objectMapper = createNestedObjectMapper("nested");
        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "alias", "nested.field");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            MappingLookup mappers = createMappingLookup(
                singletonList(createFieldMapper("nested", "field")),
                Collections.singletonList(objectMapper),
                singletonList(aliasMapper),
                emptyList()
            );
            aliasMapper.validate(mappers);
        });

        String expectedMessage = "Invalid [path] value [nested.field] for field alias [alias]: "
            + "an alias must have the same nested scope as its target. The alias is not nested, "
            + "but the target's nested scope is [nested].";
        assertEquals(expectedMessage, e.getMessage());
    }

    public void testFieldAliasWithDifferentNestedScopes() {
        FieldAliasMapper aliasMapper = new FieldAliasMapper("alias", "nested2.alias", "nested1.field");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            MappingLookup mappers = createMappingLookup(
                singletonList(createFieldMapper("nested1", "field")),
                List.of(createNestedObjectMapper("nested1"), createNestedObjectMapper("nested2")),
                singletonList(aliasMapper),
                emptyList()
            );
            aliasMapper.validate(mappers);
        });

        String expectedMessage = "Invalid [path] value [nested1.field] for field alias [nested2.alias]: "
            + "an alias must have the same nested scope as its target. The alias's nested scope is [nested2], "
            + "but the target's nested scope is [nested1].";
        assertEquals(expectedMessage, e.getMessage());
    }

    private static FieldMapper createFieldMapper(String parent, String name) {
        return new BooleanFieldMapper.Builder(name, ScriptCompiler.NONE, false, IndexVersion.current()).build(
            new MapperBuilderContext(parent, false)
        );
    }

    private static ObjectMapper createObjectMapper(String name) {
        return new ObjectMapper(name, name, Explicit.IMPLICIT_TRUE, Explicit.IMPLICIT_TRUE, ObjectMapper.Dynamic.FALSE, emptyMap());
    }

    private static NestedObjectMapper createNestedObjectMapper(String name) {
        return new NestedObjectMapper.Builder(name, IndexVersion.current()).build(MapperBuilderContext.root(false));
    }

    private static MappingLookup createMappingLookup(
        List<FieldMapper> fieldMappers,
        List<ObjectMapper> objectMappers,
        List<FieldAliasMapper> fieldAliasMappers,
        List<RuntimeField> runtimeFields
    ) {
        RootObjectMapper.Builder builder = new RootObjectMapper.Builder("_doc", ObjectMapper.Defaults.SUBOBJECTS);
        Map<String, RuntimeField> runtimeFieldTypes = runtimeFields.stream().collect(Collectors.toMap(RuntimeField::name, r -> r));
        builder.addRuntimeFields(runtimeFieldTypes);
        Mapping mapping = new Mapping(builder.build(MapperBuilderContext.root(false)), new MetadataFieldMapper[0], Collections.emptyMap());
        return MappingLookup.fromMappers(mapping, fieldMappers, objectMappers, fieldAliasMappers);
    }
}
