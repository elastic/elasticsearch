/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

public class FieldTypeLookupTests extends ESTestCase {

    public void testEmpty() {
        FieldTypeLookup lookup = new FieldTypeLookup(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        assertNull(lookup.get("foo"));
        Collection<String> names = lookup.getMatchingFieldNames("foo");
        assertNotNull(names);
        assertThat(names, hasSize(0));
    }

    public void testAddNewField() {
        MockFieldMapper f = new MockFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(f), emptyList(), Collections.emptyList());
        assertNull(lookup.get("bar"));
        assertEquals(f.fieldType(), lookup.get("foo"));
    }

    public void testAddFieldAlias() {
        MockFieldMapper field = new MockFieldMapper("foo");
        FieldAliasMapper alias = new FieldAliasMapper("alias", "alias", "foo");

        FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(field), Collections.singletonList(alias),
            Collections.emptyList());

        MappedFieldType aliasType = lookup.get("alias");
        assertEquals(field.fieldType(), aliasType);
    }

    public void testGetMatchingFieldNamesAndTypes() {
        MockFieldMapper field1 = new MockFieldMapper("foo");
        MockFieldMapper field2 = new MockFieldMapper("bar");
        MockFieldMapper field3 = new MockFieldMapper("baz");

        FieldAliasMapper alias1 = new FieldAliasMapper("food", "food", "foo");
        FieldAliasMapper alias2 = new FieldAliasMapper("barometer", "barometer", "bar");

        TestRuntimeField runtimeField = new TestRuntimeField("baz", "type");
        TestDynamicRuntimeField dynamicRuntimeField = new TestDynamicRuntimeField("baro",
            Collections.singletonMap("meter", new TestRuntimeField("meter", "test")));

        FieldTypeLookup lookup = new FieldTypeLookup(List.of(field1, field2, field3), List.of(alias1, alias2),
            List.of(runtimeField, dynamicRuntimeField));

        {
            Collection<String> names = lookup.getMatchingFieldNames("*");
            assertThat(names, containsInAnyOrder("foo", "food", "bar", "baz", "barometer", "baro.meter"));
            Collection<MappedFieldType> matchingFieldTypes = lookup.getMatchingFieldTypes("*");
            assertThat(matchingFieldTypes, hasSize(6));
            matchingFieldTypes = lookup.getMatchingFieldTypes(ft -> true);
            assertThat(matchingFieldTypes, hasSize(6));
        }
        {
            Collection<String> names = lookup.getMatchingFieldNames("b*");
            assertThat(names, containsInAnyOrder("bar", "baz", "barometer", "baro.meter"));
            Collection<MappedFieldType> fieldTypes = lookup.getMatchingFieldTypes("b*");
            assertThat(fieldTypes, hasSize(4));     // both "bar" and "barometer" get returned as field types
            Set<String> matchedNames = fieldTypes.stream().map(MappedFieldType::name).collect(Collectors.toSet());
            assertThat(matchedNames, containsInAnyOrder("bar", "baz", "meter"));
        }
        {
            Collection<String> names = lookup.getMatchingFieldNames("baro.anything");
            assertThat(names, containsInAnyOrder("baro.anything"));
            Collection<MappedFieldType> matchingFieldTypes = lookup.getMatchingFieldTypes("baro.anything");
            assertThat(matchingFieldTypes, hasSize(1));
        }
        {
            Collection<String> names = lookup.getMatchingFieldNames("baro.any*");
            assertThat(names, hasSize(0));
            Collection<MappedFieldType> matchingFieldTypes = lookup.getMatchingFieldTypes("baro.any*");
            assertThat(matchingFieldTypes, hasSize(0));
        }
        {
            Collection<String> names = lookup.getMatchingFieldNames("foo*");
            assertThat(names, containsInAnyOrder("foo", "food"));
            Collection<MappedFieldType> matchingFieldTypes = lookup.getMatchingFieldTypes("foo*");
            assertThat(matchingFieldTypes, hasSize(2));
            for (MappedFieldType matchingFieldType : matchingFieldTypes) {
                assertThat(matchingFieldType.name(), equalTo("foo"));
            }
            matchingFieldTypes = lookup.getMatchingFieldTypes(ft -> ft.name().startsWith("foo"));
            assertThat(matchingFieldTypes, hasSize(2));
            for (MappedFieldType matchingFieldType : matchingFieldTypes) {
                assertThat(matchingFieldType.name(), equalTo("foo"));
            }
        }
        {
            Collection<MappedFieldType> matchingFieldTypes = lookup.getMatchingFieldTypes(ft -> ft.name().equals("meter"));
            assertThat(matchingFieldTypes, hasSize(1));
        }
    }

    public void testSourcePathWithMultiFields() {
        MockFieldMapper field = new MockFieldMapper.Builder("field")
            .addMultiField(new MockFieldMapper.Builder("field.subfield1"))
            .addMultiField(new MockFieldMapper.Builder("field.subfield2"))
            .build(new ContentPath());

        FieldTypeLookup lookup = new FieldTypeLookup(singletonList(field), emptyList(), emptyList());

        assertEquals(Set.of("field"), lookup.sourcePaths("field"));
        assertEquals(Set.of("field"), lookup.sourcePaths("field.subfield1"));
        assertEquals(Set.of("field"), lookup.sourcePaths("field.subfield2"));
    }

    public void testSourcePathsWithCopyTo() {
        MockFieldMapper field = new MockFieldMapper.Builder("field")
            .addMultiField(new MockFieldMapper.Builder("field.subfield1"))
            .build(new ContentPath());

        MockFieldMapper otherField = new MockFieldMapper.Builder("other_field")
            .copyTo("field")
            .build(new ContentPath());

        FieldTypeLookup lookup = new FieldTypeLookup(Arrays.asList(field, otherField), emptyList(), emptyList());

        assertEquals(Set.of("other_field", "field"), lookup.sourcePaths("field"));
        assertEquals(Set.of("other_field", "field"), lookup.sourcePaths("field.subfield1"));
    }

    public void testRuntimeFieldsLookup() {
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField runtime = new TestRuntimeField("runtime", "type");

        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(List.of(concrete), emptyList(), List.of(runtime));
        assertThat(fieldTypeLookup.get("concrete"), instanceOf(MockFieldMapper.FakeFieldType.class));
        assertThat(fieldTypeLookup.get("runtime"), instanceOf(TestRuntimeField.class));
    }

    public void testRuntimeFieldsOverrideConcreteFields() {
        FlattenedFieldMapper flattened = createFlattenedMapper("flattened");
        MockFieldMapper field = new MockFieldMapper("field");
        MockFieldMapper subfield = new MockFieldMapper("object.subfield");
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField fieldOverride = new TestRuntimeField("field", "type");
        TestRuntimeField subfieldOverride = new TestRuntimeField("object.subfield", "type");
        TestRuntimeField runtime = new TestRuntimeField("runtime", "type");
        TestDynamicRuntimeField dynamicRuntimeField = new TestDynamicRuntimeField("flattened",
            Collections.singletonMap("sub", new TestRuntimeField("sub", "ip")));

        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(List.of(field, concrete, subfield, flattened), emptyList(),
            List.of(fieldOverride, runtime, subfieldOverride, dynamicRuntimeField));
        assertThat(fieldTypeLookup.get("field"), instanceOf(TestRuntimeField.class));
        assertThat(fieldTypeLookup.get("object.subfield"), instanceOf(TestRuntimeField.class));
        assertThat(fieldTypeLookup.get("concrete"), instanceOf(MockFieldMapper.FakeFieldType.class));
        assertThat(fieldTypeLookup.get("runtime"), instanceOf(TestRuntimeField.class));
        assertThat(fieldTypeLookup.get("flattened.sub").typeName(), equalTo("ip"));
    }

    public void testRuntimeFieldsSourcePaths() {
        //we test that runtime fields are treated like any other field by sourcePaths, although sourcePaths
        // should never be called for runtime fields as they are not in _source
        MockFieldMapper field1 = new MockFieldMapper("field1");
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField field2 = new TestRuntimeField("field2", "type");
        TestRuntimeField subfield = new TestRuntimeField("object.subfield", "type");

        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(List.of(field1, concrete), emptyList(), List.of(field2, subfield));
        {
            Set<String> sourcePaths = fieldTypeLookup.sourcePaths("field1");
            assertEquals(1, sourcePaths.size());
            assertTrue(sourcePaths.contains("field1"));
        }
        {
            Set<String> sourcePaths = fieldTypeLookup.sourcePaths("field2");
            assertEquals(1, sourcePaths.size());
            assertTrue(sourcePaths.contains("field2"));
        }
        {
            Set<String> sourcePaths = fieldTypeLookup.sourcePaths("object.subfield");
            assertEquals(1, sourcePaths.size());
            assertTrue(sourcePaths.contains("object.subfield"));
        }
    }

    public void testDynamicRuntimeFields() {
        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(emptyList(), emptyList(),
            Collections.singletonList(new TestDynamicRuntimeField("test")));

        assertNull(fieldTypeLookup.get("test"));
        assertEquals(0, fieldTypeLookup.getMatchingFieldTypes("test").size());
        assertEquals(0, fieldTypeLookup.getMatchingFieldNames("test").size());

        String fieldName = "test." + randomAlphaOfLengthBetween(3, 6);
        assertEquals(KeywordFieldMapper.CONTENT_TYPE, fieldTypeLookup.get(fieldName).typeName());
        Collection<MappedFieldType> matchingFieldTypes = fieldTypeLookup.getMatchingFieldTypes(fieldName);
        assertEquals(1, matchingFieldTypes.size());
        assertEquals(KeywordFieldMapper.CONTENT_TYPE, matchingFieldTypes.iterator().next().typeName());
        Set<String> matchingFieldNames = fieldTypeLookup.getMatchingFieldNames(fieldName);
        assertEquals(1, matchingFieldTypes.size());
        assertEquals(fieldName, matchingFieldNames.iterator().next());
    }

    public void testFlattenedLookup() {
        String fieldName = "object1.object2.field";
        FlattenedFieldMapper mapper = createFlattenedMapper(fieldName);

        FieldTypeLookup lookup = new FieldTypeLookup(singletonList(mapper), emptyList(), emptyList());
        assertEquals(mapper.fieldType(), lookup.get(fieldName));

        String objectKey = "key1.key2";
        String searchFieldName = fieldName + "." + objectKey;

        MappedFieldType searchFieldType = lookup.get(searchFieldName);
        assertNotNull(searchFieldType);
        assertThat(searchFieldType, Matchers.instanceOf(FlattenedFieldMapper.KeyedFlattenedFieldType.class));
        FlattenedFieldMapper.KeyedFlattenedFieldType keyedFieldType = (FlattenedFieldMapper.KeyedFlattenedFieldType) searchFieldType;
        assertEquals(objectKey, keyedFieldType.key());

        assertThat(lookup.getMatchingFieldNames("object1.*"), contains("object1.object2.field"));
        // We can directly find dynamic subfields
        assertThat(lookup.getMatchingFieldNames("object1.object2.field.foo"), contains("object1.object2.field.foo"));
        // But you can't generate dynamic subfields from a wildcard pattern
        assertThat(lookup.getMatchingFieldNames("object1.object2.field.foo*"), hasSize(0));
    }

    public void testFlattenedLookupWithAlias() {
        String fieldName = "object1.object2.field";
        FlattenedFieldMapper mapper = createFlattenedMapper(fieldName);

        String aliasName = "alias";
        FieldAliasMapper alias = new FieldAliasMapper(aliasName, aliasName, fieldName);

        FieldTypeLookup lookup = new FieldTypeLookup(singletonList(mapper), singletonList(alias), emptyList());
        assertEquals(mapper.fieldType(), lookup.get(aliasName));

        String objectKey = "key1.key2";
        String searchFieldName = aliasName + "." + objectKey;

        MappedFieldType searchFieldType = lookup.get(searchFieldName);
        assertNotNull(searchFieldType);
        assertThat(searchFieldType, Matchers.instanceOf(FlattenedFieldMapper.KeyedFlattenedFieldType.class));
        FlattenedFieldMapper.KeyedFlattenedFieldType keyedFieldType = (FlattenedFieldMapper.KeyedFlattenedFieldType) searchFieldType;
        assertEquals(objectKey, keyedFieldType.key());
    }

    public void testFlattenedLookupWithMultipleFields() {
        String field1 = "object1.object2.field";
        String field2 = "object1.field";
        String field3 = "object2.field";

        FlattenedFieldMapper mapper1 = createFlattenedMapper(field1);
        FlattenedFieldMapper mapper2 = createFlattenedMapper(field2);
        FlattenedFieldMapper mapper3 = createFlattenedMapper(field3);

        FieldTypeLookup lookup = new FieldTypeLookup(Arrays.asList(mapper1, mapper2), emptyList(), emptyList());
        assertNotNull(lookup.get(field1 + ".some.key"));
        assertNotNull(lookup.get(field2 + ".some.key"));

        lookup = new FieldTypeLookup(Arrays.asList(mapper1, mapper2, mapper3), emptyList(), emptyList());
        assertNotNull(lookup.get(field1 + ".some.key"));
        assertNotNull(lookup.get(field2 + ".some.key"));
        assertNotNull(lookup.get(field3 + ".some.key"));
    }

    public void testUnmappedLookupWithDots() {
        FieldTypeLookup lookup = new FieldTypeLookup(emptyList(), emptyList(), emptyList());
        assertNull(lookup.get("object.child"));
    }

    public void testMaxDynamicKeyDepth() {
        {
            FieldTypeLookup lookup = new FieldTypeLookup(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
            assertEquals(0, lookup.getMaxParentPathDots());
        }

        // Add a flattened object field.
        {
            String name = "object1.object2.field";
            FieldTypeLookup lookup = new FieldTypeLookup(
                Collections.singletonList(createFlattenedMapper(name)),
                Collections.emptyList(),
                Collections.emptyList()
            );
            assertEquals(2, lookup.getMaxParentPathDots());
        }

        // Add a short alias to that field.
        {
            String name = "object1.object2.field";
            FieldTypeLookup lookup = new FieldTypeLookup(
                Collections.singletonList(createFlattenedMapper(name)),
                Collections.singletonList(new FieldAliasMapper("alias", "alias", "object1.object2.field")),
                Collections.emptyList()
            );
            assertEquals(2, lookup.getMaxParentPathDots());
        }

        // Add a longer alias to that field.
        {
            String name = "object1.object2.field";
            FieldTypeLookup lookup = new FieldTypeLookup(
                Collections.singletonList(createFlattenedMapper(name)),
                Collections.singletonList(new FieldAliasMapper("alias", "object1.object2.object3.alias", "object1.object2.field")),
                Collections.emptyList()
            );
            assertEquals(2, lookup.getMaxParentPathDots());
        }
    }

    private static FlattenedFieldMapper createFlattenedMapper(String fieldName) {
        return new FlattenedFieldMapper.Builder(fieldName).build(new ContentPath());
    }
}
