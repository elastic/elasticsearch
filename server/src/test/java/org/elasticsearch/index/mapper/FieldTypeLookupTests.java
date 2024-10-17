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
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

public class FieldTypeLookupTests extends ESTestCase {

    public void testEmpty() {
        FieldTypeLookup lookup = new FieldTypeLookup(emptyList(), emptyList());
        assertNull(lookup.get("foo"));
        Collection<String> names = lookup.getMatchingFieldNames("foo");
        assertNotNull(names);
        assertThat(names, hasSize(0));
    }

    public void testAddNewField() {
        MockFieldMapper f = new MockFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(f), emptyList());
        assertNull(lookup.get("bar"));
        assertEquals(f.fieldType(), lookup.get("foo"));
    }

    public void testAddFieldAlias() {
        MockFieldMapper field = new MockFieldMapper("foo");
        FieldAliasMapper alias = new FieldAliasMapper("alias", "alias", "foo");

        FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(field), Collections.singletonList(alias));

        MappedFieldType aliasType = lookup.get("alias");
        assertEquals(field.fieldType(), aliasType);
    }

    public void testGetMatchingFieldNames() {
        FlattenedFieldMapper flattened = createFlattenedMapper("flattened");
        MockFieldMapper field1 = new MockFieldMapper("foo");
        MockFieldMapper field2 = new MockFieldMapper("bar");
        MockFieldMapper field3 = new MockFieldMapper("baz");

        FieldAliasMapper alias1 = new FieldAliasMapper("food", "food", "foo");
        FieldAliasMapper alias2 = new FieldAliasMapper("barometer", "barometer", "bar");

        TestRuntimeField runtimeField = new TestRuntimeField("baz", "type");
        TestRuntimeField multi = new TestRuntimeField(
            "flat",
            List.of(
                new TestRuntimeField.TestRuntimeFieldType("flat.first", "first"),
                new TestRuntimeField.TestRuntimeFieldType("flat.second", "second")
            )
        );

        FieldTypeLookup lookup = new FieldTypeLookup(
            List.of(field1, field2, field3, flattened),
            List.of(alias1, alias2),
            List.of(),
            List.of(runtimeField, multi)
        );

        {
            Collection<String> names = lookup.getMatchingFieldNames("*");
            assertThat(names, containsInAnyOrder("foo", "food", "bar", "baz", "barometer", "flattened", "flat.first", "flat.second"));
        }
        {
            Collection<String> names = lookup.getMatchingFieldNames("b*");
            assertThat(names, containsInAnyOrder("bar", "baz", "barometer"));
        }
        {
            Collection<String> names = lookup.getMatchingFieldNames("fl*");
            assertThat(names, containsInAnyOrder("flattened", "flat.first", "flat.second"));
        }
        {
            Collection<String> names = lookup.getMatchingFieldNames("baro.any*");
            assertThat(names, hasSize(0));
        }
        {
            Collection<String> names = lookup.getMatchingFieldNames("foo*");
            assertThat(names, containsInAnyOrder("foo", "food"));
        }
        {
            Collection<String> names = lookup.getMatchingFieldNames("flattened.anything");
            assertThat(names, containsInAnyOrder("flattened.anything"));
        }
        {
            Collection<String> names = lookup.getMatchingFieldNames("flat.first");
            assertThat(names, containsInAnyOrder("flat.first"));
        }
        {
            Collection<String> names = lookup.getMatchingFieldNames("flat.second");
            assertThat(names, containsInAnyOrder("flat.second"));
        }
    }

    public void testSourcePathWithMultiFields() {
        MockFieldMapper field = new MockFieldMapper.Builder("field").addMultiField(new MockFieldMapper.Builder("field.subfield1"))
            .addMultiField(new MockFieldMapper.Builder("field.subfield2.subfield3"))
            .build(MapperBuilderContext.root(false, false));

        // Adding a subfield that is not multi-field
        MockFieldMapper subfield = new MockFieldMapper.Builder("field.subfield4").build(MapperBuilderContext.root(false, false));

        FieldTypeLookup lookup = new FieldTypeLookup(List.of(field, subfield), emptyList());

        assertEquals(Set.of("field"), lookup.sourcePaths("field"));
        assertEquals(Set.of("field"), lookup.sourcePaths("field.subfield1"));
        assertEquals(Set.of("field"), lookup.sourcePaths("field.subfield2.subfield3"));
        assertEquals(Set.of("field.subfield4"), lookup.sourcePaths("field.subfield4"));
    }

    public void testSourcePathsWithCopyTo() {
        MockFieldMapper field = new MockFieldMapper.Builder("field").addMultiField(new MockFieldMapper.Builder("field.subfield1"))
            .build(MapperBuilderContext.root(false, false));

        MockFieldMapper nestedField = new MockFieldMapper.Builder("field.nested").addMultiField(
            new MockFieldMapper.Builder("field.nested.subfield1")
        ).build(MapperBuilderContext.root(false, false));

        MockFieldMapper otherField = new MockFieldMapper.Builder("other_field").copyTo("field")
            .copyTo("field.nested")
            .build(MapperBuilderContext.root(false, false));

        MockFieldMapper otherNestedField = new MockFieldMapper.Builder("other_field.nested").copyTo("field")
            .copyTo("field.nested")
            .build(MapperBuilderContext.root(false, false));

        FieldTypeLookup lookup = new FieldTypeLookup(Arrays.asList(field, nestedField, otherField, otherNestedField), emptyList());

        assertEquals(Set.of("other_field", "other_field.nested", "field"), lookup.sourcePaths("field"));
        assertEquals(Set.of("other_field", "other_field.nested", "field"), lookup.sourcePaths("field.subfield1"));
        assertEquals(Set.of("other_field", "other_field.nested", "field.nested"), lookup.sourcePaths("field.nested"));
        assertEquals(Set.of("other_field", "other_field.nested", "field.nested"), lookup.sourcePaths("field.nested.subfield1"));
    }

    public void testRuntimeFieldsLookup() {
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField runtimeLong = new TestRuntimeField("multi.outside", "date");
        TestRuntimeField runtime = new TestRuntimeField("string", "type");
        TestRuntimeField multi = new TestRuntimeField(
            "multi",
            List.of(
                new TestRuntimeField.TestRuntimeFieldType("multi.string", "string"),
                new TestRuntimeField.TestRuntimeFieldType("multi.long", "long")
            )
        );

        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(
            List.of(concrete),
            emptyList(),
            emptyList(),
            List.of(runtime, runtimeLong, multi)
        );
        assertThat(fieldTypeLookup.get("concrete"), instanceOf(MockFieldMapper.FakeFieldType.class));
        assertThat(fieldTypeLookup.get("string"), instanceOf(TestRuntimeField.TestRuntimeFieldType.class));
        assertThat(fieldTypeLookup.get("string").typeName(), equalTo("type"));
        assertThat(fieldTypeLookup.get("multi"), nullValue());
        assertThat(fieldTypeLookup.get("multi.string"), instanceOf(TestRuntimeField.TestRuntimeFieldType.class));
        assertThat(fieldTypeLookup.get("multi.string").typeName(), equalTo("string"));
        assertThat(fieldTypeLookup.get("multi.long"), instanceOf(TestRuntimeField.TestRuntimeFieldType.class));
        assertThat(fieldTypeLookup.get("multi.long").typeName(), equalTo("long"));
        assertThat(fieldTypeLookup.get("multi.outside"), instanceOf(TestRuntimeField.TestRuntimeFieldType.class));
        assertThat(fieldTypeLookup.get("multi.outside").typeName(), equalTo("date"));
        assertThat(fieldTypeLookup.get("multi.anything"), nullValue());
    }

    public void testRuntimeFieldsOverrideConcreteFields() {
        FlattenedFieldMapper flattened = createFlattenedMapper("flattened");
        MockFieldMapper field = new MockFieldMapper("field");
        MockFieldMapper subfield = new MockFieldMapper("object.subfield");
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField fieldOverride = new TestRuntimeField("field", "string");
        TestRuntimeField subfieldOverride = new TestRuntimeField(
            "object",
            Collections.singleton(new TestRuntimeField.TestRuntimeFieldType("object.subfield", "leaf"))
        );
        TestRuntimeField runtime = new TestRuntimeField("runtime", "type");
        TestRuntimeField flattenedRuntime = new TestRuntimeField("flattened.runtime", "type");

        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(
            List.of(field, concrete, subfield, flattened),
            emptyList(),
            emptyList(),
            List.of(fieldOverride, runtime, subfieldOverride, flattenedRuntime)
        );
        assertThat(fieldTypeLookup.get("field"), instanceOf(TestRuntimeField.TestRuntimeFieldType.class));
        assertThat(fieldTypeLookup.get("field").typeName(), equalTo("string"));
        assertThat(fieldTypeLookup.get("object.subfield"), instanceOf(TestRuntimeField.TestRuntimeFieldType.class));
        assertThat(fieldTypeLookup.get("object.subfield").typeName(), equalTo("leaf"));
        assertThat(fieldTypeLookup.get("concrete"), instanceOf(MockFieldMapper.FakeFieldType.class));
        assertThat(fieldTypeLookup.get("runtime"), instanceOf(TestRuntimeField.TestRuntimeFieldType.class));
        assertThat(fieldTypeLookup.get("runtime").typeName(), equalTo("type"));
        assertThat(fieldTypeLookup.get("flattened.anything"), instanceOf(FlattenedFieldMapper.KeyedFlattenedFieldType.class));
        assertThat(fieldTypeLookup.get("flattened.runtime"), instanceOf(TestRuntimeField.TestRuntimeFieldType.class));
    }

    public void testRuntimeFieldsSourcePaths() {
        // we test that runtime fields are treated like any other field by sourcePaths, although sourcePaths
        // should never be called for runtime fields as they are not in _source
        MockFieldMapper field1 = new MockFieldMapper("field1");
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField field2 = new TestRuntimeField("field2", "type");
        TestRuntimeField subfield = new TestRuntimeField("object.subfield", "type");

        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(
            List.of(field1, concrete),
            emptyList(),
            emptyList(),
            List.of(field2, subfield)
        );
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

    public void testFlattenedLookup() {
        String fieldName = "object1.object2.field";
        FlattenedFieldMapper mapper = createFlattenedMapper(fieldName);

        FieldTypeLookup lookup = new FieldTypeLookup(singletonList(mapper), emptyList());
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

        FieldTypeLookup lookup = new FieldTypeLookup(singletonList(mapper), singletonList(alias), emptyList(), emptyList());
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

        FieldTypeLookup lookup = new FieldTypeLookup(Arrays.asList(mapper1, mapper2), emptyList());
        assertNotNull(lookup.get(field1 + ".some.key"));
        assertNotNull(lookup.get(field2 + ".some.key"));

        lookup = new FieldTypeLookup(Arrays.asList(mapper1, mapper2, mapper3), emptyList());
        assertNotNull(lookup.get(field1 + ".some.key"));
        assertNotNull(lookup.get(field2 + ".some.key"));
        assertNotNull(lookup.get(field3 + ".some.key"));
    }

    public void testUnmappedLookupWithDots() {
        FieldTypeLookup lookup = new FieldTypeLookup(emptyList(), emptyList());
        assertNull(lookup.get("object.child"));
    }

    public void testMaxDynamicKeyDepth() {
        {
            FieldTypeLookup lookup = new FieldTypeLookup(emptyList(), emptyList());
            assertEquals(0, lookup.getMaxParentPathDots());
        }

        // Add a flattened object field.
        {
            String name = "object1.object2.field";
            FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(createFlattenedMapper(name)), emptyList());
            assertEquals(2, lookup.getMaxParentPathDots());
        }

        // Add a short alias to that field.
        {
            String name = "object1.object2.field";
            FieldTypeLookup lookup = new FieldTypeLookup(
                Collections.singletonList(createFlattenedMapper(name)),
                Collections.singletonList(new FieldAliasMapper("alias", "alias", "object1.object2.field"))
            );
            assertEquals(2, lookup.getMaxParentPathDots());
        }

        // Add a longer alias to that field.
        {
            String name = "object1.object2.field";
            FieldTypeLookup lookup = new FieldTypeLookup(
                Collections.singletonList(createFlattenedMapper(name)),
                Collections.singletonList(new FieldAliasMapper("alias", "object1.object2.object3.alias", "object1.object2.field"))
            );
            assertEquals(2, lookup.getMaxParentPathDots());
        }
    }

    public void testRuntimeFieldNameClashes() {
        {
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> new FieldTypeLookup(
                    emptyList(),
                    emptyList(),
                    emptyList(),
                    List.of(new TestRuntimeField("field", "type"), new TestRuntimeField("field", "long"))
                )
            );
            assertEquals(iae.getMessage(), "Found two runtime fields with same name [field]");
        }
        {
            TestRuntimeField multi = new TestRuntimeField(
                "multi",
                Collections.singleton(new TestRuntimeField.TestRuntimeFieldType("multi.first", "leaf"))
            );
            TestRuntimeField runtime = new TestRuntimeField("multi.first", "runtime");
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> new FieldTypeLookup(emptyList(), emptyList(), emptyList(), List.of(multi, runtime))
            );
            assertEquals(iae.getMessage(), "Found two runtime fields with same name [multi.first]");
        }
        {
            TestRuntimeField multi = new TestRuntimeField(
                "multi",
                List.of(
                    new TestRuntimeField.TestRuntimeFieldType("multi", "leaf"),
                    new TestRuntimeField.TestRuntimeFieldType("multi", "leaf")
                )
            );

            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> new FieldTypeLookup(emptyList(), emptyList(), emptyList(), List.of(multi))
            );
            assertEquals(iae.getMessage(), "Found two runtime fields with same name [multi]");
        }
    }

    public void testRuntimeFieldNameOutsideContext() {
        {
            TestRuntimeField multi = new TestRuntimeField(
                "multi",
                List.of(
                    new TestRuntimeField.TestRuntimeFieldType("first", "leaf"),
                    new TestRuntimeField.TestRuntimeFieldType("second", "leaf"),
                    new TestRuntimeField.TestRuntimeFieldType("multi.third", "leaf")
                )
            );
            IllegalStateException ise = expectThrows(
                IllegalStateException.class,
                () -> new FieldTypeLookup(emptyList(), emptyList(), emptyList(), Collections.singletonList(multi))
            );
            assertEquals("Found sub-fields with name not belonging to the parent field they are part of [first, second]", ise.getMessage());
        }
        {
            TestRuntimeField multi = new TestRuntimeField(
                "multi",
                List.of(
                    new TestRuntimeField.TestRuntimeFieldType("multi.", "leaf"),
                    new TestRuntimeField.TestRuntimeFieldType("multi.f", "leaf")
                )
            );
            IllegalStateException ise = expectThrows(
                IllegalStateException.class,
                () -> new FieldTypeLookup(emptyList(), emptyList(), emptyList(), Collections.singletonList(multi))
            );
            assertEquals("Found sub-fields with name not belonging to the parent field they are part of [multi.]", ise.getMessage());
        }
    }

    private static FlattenedFieldMapper createFlattenedMapper(String fieldName) {
        return new FlattenedFieldMapper.Builder(fieldName).build(MapperBuilderContext.root(false, false));
    }

    private PassThroughObjectMapper createPassThroughMapper(String name, Map<String, Mapper> mappers, int priority) {
        return new PassThroughObjectMapper(
            name,
            name,
            Explicit.EXPLICIT_TRUE,
            Optional.empty(),
            ObjectMapper.Dynamic.FALSE,
            mappers,
            Explicit.EXPLICIT_FALSE,
            priority
        );
    }

    public void testAddRootAliasesForPassThroughFields() {
        MockFieldMapper foo = new MockFieldMapper("attributes.foo");
        MockFieldMapper bar = new MockFieldMapper(
            new MockFieldMapper.FakeFieldType("attributes.much.more.deeply.nested.bar"),
            "much.more.deeply.nested.bar"
        );
        PassThroughObjectMapper attributes = createPassThroughMapper(
            "attributes",
            Map.of("foo", foo, "much.more.deeply.nested.bar", bar),
            0
        );

        MockFieldMapper baz = new MockFieldMapper("resource.attributes.baz");
        MockFieldMapper bag = new MockFieldMapper(
            new MockFieldMapper.FakeFieldType("resource.attributes.much.more.deeply.nested.bag"),
            "much.more.deeply.nested.bag"
        );
        PassThroughObjectMapper resourceAttributes = createPassThroughMapper(
            "resource.attributes",
            Map.of("baz", baz, "much.more.deeply.nested.bag", bag),
            1
        );

        FieldTypeLookup lookup = new FieldTypeLookup(
            List.of(foo, bar, baz, bag),
            List.of(),
            List.of(attributes, resourceAttributes),
            List.of()
        );
        assertEquals(foo.fieldType(), lookup.get("foo"));
        assertEquals(bar.fieldType(), lookup.get("much.more.deeply.nested.bar"));
        assertEquals(baz.fieldType(), lookup.get("baz"));
        assertEquals(bag.fieldType(), lookup.get("much.more.deeply.nested.bag"));
    }

    public void testNoPassThroughField() {
        MockFieldMapper field = new MockFieldMapper("labels.foo");
        PassThroughObjectMapper attributes = createPassThroughMapper("attributes", Map.of(), 0);

        FieldTypeLookup lookup = new FieldTypeLookup(List.of(field), List.of(), List.of(attributes), List.of());
        assertNull(lookup.get("foo"));
    }

    public void testAddRootAliasForConflictingPassThroughFields() {
        MockFieldMapper attributeField = new MockFieldMapper("attributes.foo");
        PassThroughObjectMapper attributes = createPassThroughMapper("attributes", Map.of("foo", attributeField), 1);

        MockFieldMapper resourceAttributeField = new MockFieldMapper("resource.attributes.foo");
        PassThroughObjectMapper resourceAttributes = createPassThroughMapper(
            "resource.attributes",
            Map.of("foo", resourceAttributeField),
            0
        );

        FieldTypeLookup lookup = new FieldTypeLookup(
            randomizedList(attributeField, resourceAttributeField),
            List.of(),
            randomizedList(attributes, resourceAttributes),
            List.of()
        );
        assertEquals(attributeField.fieldType(), lookup.get("foo"));
    }

    public void testNoRootAliasForPassThroughFieldOnConflictingField() {
        MockFieldMapper attributeFoo = new MockFieldMapper("attributes.foo");
        MockFieldMapper resourceAttributeFoo = new MockFieldMapper("resource.attributes.foo");
        MockFieldMapper foo = new MockFieldMapper("foo");
        PassThroughObjectMapper attributes = createPassThroughMapper("attributes", Map.of("foo", attributeFoo), 0);
        PassThroughObjectMapper resourceAttributes = createPassThroughMapper("resource.attributes", Map.of("foo", resourceAttributeFoo), 1);

        FieldTypeLookup lookup = new FieldTypeLookup(
            randomizedList(foo, attributeFoo, resourceAttributeFoo),
            List.of(),
            randomizedList(attributes, resourceAttributes),
            List.of()
        );

        assertEquals(foo.fieldType(), lookup.get("foo"));
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    static <T> List<T> randomizedList(T... values) {
        ArrayList<T> list = new ArrayList<>(Arrays.asList(values));
        Collections.shuffle(list, random());
        return list;
    }
}
