/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;

public class FieldTypeLookupTests extends ESTestCase {

    public void testEmpty() {
        FieldTypeLookup lookup = new FieldTypeLookup(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        assertNull(lookup.get("foo"));
        Collection<String> names = lookup.simpleMatchToFullName("foo");
        assertNotNull(names);
        assertThat(names, equalTo(Set.of("foo")));
        assertEquals(0, size(lookup.filter(ft -> true)));
    }

    public void testFilter() {
        Collection<FieldMapper> fieldMappers = List.of(new MockFieldMapper("field"), new MockFieldMapper("test"));
        Collection<FieldAliasMapper> fieldAliases = singletonList(new FieldAliasMapper("alias", "alias", "test"));
        Collection<RuntimeFieldType> runtimeFields = List.of(
            new TestRuntimeField("runtime", "type"), new TestRuntimeField("field", "type"));
        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(fieldMappers, fieldAliases, runtimeFields);
        assertEquals(3, size(fieldTypeLookup.filter(ft -> true)));
        for (MappedFieldType fieldType : fieldTypeLookup.filter(ft -> true)) {
            if (fieldType.name().equals("test")) {
                assertThat(fieldType, instanceOf(MockFieldMapper.FakeFieldType.class));
            }
            if (fieldType.name().equals("field") || fieldType.name().equals("runtime")) {
                assertThat(fieldType, instanceOf(TestRuntimeField.class));
            }
        }
        assertEquals(0, size(fieldTypeLookup.filter(ft -> false)));
        {
            Iterable<MappedFieldType> fieldIterable = fieldTypeLookup.filter(ft -> ft.name().equals("field"));
            assertEquals(1, size(fieldIterable));
            MappedFieldType field = fieldIterable.iterator().next();
            assertEquals("field", field.name());
            assertThat(field, instanceOf(TestRuntimeField.class));
        }
        {
            Iterable<MappedFieldType> fieldIterable = fieldTypeLookup.filter(ft -> ft.name().equals("test"));
            assertEquals(1, size(fieldIterable));
            MappedFieldType field = fieldIterable.iterator().next();
            assertEquals("test", field.name());
            assertThat(field, instanceOf(MockFieldMapper.FakeFieldType.class));
        }
    }

    public void testAddNewField() {
        MockFieldMapper f = new MockFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(f), emptyList(), Collections.emptyList());
        assertNull(lookup.get("bar"));
        assertEquals(f.fieldType(), lookup.get("foo"));
        assertEquals(1, size(lookup.filter(ft -> true)));
    }

    public void testAddFieldAlias() {
        MockFieldMapper field = new MockFieldMapper("foo");
        FieldAliasMapper alias = new FieldAliasMapper("alias", "alias", "foo");

        FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(field), Collections.singletonList(alias),
            Collections.emptyList());

        MappedFieldType aliasType = lookup.get("alias");
        assertEquals(field.fieldType(), aliasType);
    }

    public void testSimpleMatchToFullName() {
        MockFieldMapper field1 = new MockFieldMapper("foo");
        MockFieldMapper field2 = new MockFieldMapper("bar");

        FieldAliasMapper alias1 = new FieldAliasMapper("food", "food", "foo");
        FieldAliasMapper alias2 = new FieldAliasMapper("barometer", "barometer", "bar");

        FieldTypeLookup lookup = new FieldTypeLookup(List.of(field1, field2), List.of(alias1, alias2), List.of());

        Collection<String> names = lookup.simpleMatchToFullName("b*");

        assertFalse(names.contains("foo"));
        assertFalse(names.contains("food"));

        assertTrue(names.contains("bar"));
        assertTrue(names.contains("barometer"));
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
        assertEquals(2, size(fieldTypeLookup.filter(ft -> true)));
    }

    public void testRuntimeFieldOverrides() {
        MockFieldMapper field = new MockFieldMapper("field");
        MockFieldMapper subfield = new MockFieldMapper("object.subfield");
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField fieldOverride = new TestRuntimeField("field", "type");
        TestRuntimeField subfieldOverride = new TestRuntimeField("object.subfield", "type");
        TestRuntimeField runtime = new TestRuntimeField("runtime", "type");

        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(List.of(field, concrete, subfield), emptyList(),
            List.of(fieldOverride, runtime, subfieldOverride));
        assertThat(fieldTypeLookup.get("field"), instanceOf(TestRuntimeField.class));
        assertThat(fieldTypeLookup.get("object.subfield"), instanceOf(TestRuntimeField.class));
        assertThat(fieldTypeLookup.get("concrete"), instanceOf(MockFieldMapper.FakeFieldType.class));
        assertThat(fieldTypeLookup.get("runtime"), instanceOf(TestRuntimeField.class));
        assertEquals(4, size(fieldTypeLookup.filter(ft -> true)));
    }

    public void testRuntimeFieldsSimpleMatchToFullName() {
        MockFieldMapper field1 = new MockFieldMapper("field1");
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField field2 = new TestRuntimeField("field2", "type");
        TestRuntimeField subfield = new TestRuntimeField("object.subfield", "type");

        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(List.of(field1, concrete), emptyList(), List.of(field2, subfield));
        {
            Set<String> matches = fieldTypeLookup.simpleMatchToFullName("fie*");
            assertEquals(2, matches.size());
            assertTrue(matches.contains("field1"));
            assertTrue(matches.contains("field2"));
        }
        {
            Set<String> matches = fieldTypeLookup.simpleMatchToFullName("object.sub*");
            assertEquals(1, matches.size());
            assertTrue(matches.contains("object.subfield"));
        }
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

    private static int size(Iterable<MappedFieldType> iterable) {
        int count = 0;
        for (MappedFieldType fieldType : iterable) {
            count++;
        }
        return count;
    }
}
