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

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;

public class FieldTypeLookupTests extends ESTestCase {

    public void testEmpty() {
        FieldTypeLookup lookup = new FieldTypeLookup(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        assertNull(lookup.get("foo"));
        Collection<String> names = lookup.simpleMatchToFullName("foo");
        assertNotNull(names);
        assertTrue(names.isEmpty());
        Iterator<MappedFieldType> itr = lookup.iterator();
        assertNotNull(itr);
        assertFalse(itr.hasNext());
    }

    public void testAddNewField() {
        MockFieldMapper f = new MockFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(f), emptyList(), Collections.emptyList());
        assertNull(lookup.get("bar"));
        assertEquals(f.fieldType(), lookup.get("foo"));
        assertEquals(1, size(lookup.iterator()));
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

        FieldTypeLookup lookup = new FieldTypeLookup(Arrays.asList(field1, field2), Arrays.asList(alias1, alias2), Collections.emptyList());

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

    public void testIteratorImmutable() {
        MockFieldMapper f1 = new MockFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(f1), emptyList(), emptyList());

        Iterator<MappedFieldType> itr = lookup.iterator();
        assertTrue(itr.hasNext());
        assertEquals(f1.fieldType(), itr.next());
        expectThrows(UnsupportedOperationException.class, itr::remove);
    }

    public void testRuntimeFieldsLookup() {
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField runtime = new TestRuntimeField("runtime");

        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(List.of(concrete), emptyList(), List.of(runtime));
        assertThat(fieldTypeLookup.get("concrete"), instanceOf(MockFieldMapper.FakeFieldType.class));
        assertThat(fieldTypeLookup.get("runtime"), instanceOf(TestRuntimeField.class));
        assertEquals(2, size(fieldTypeLookup.iterator()));
    }

    public void testRuntimeFieldOverrides() {
        MockFieldMapper field = new MockFieldMapper("field");
        MockFieldMapper subfield = new MockFieldMapper("object.subfield");
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField fieldOverride = new TestRuntimeField("field");
        TestRuntimeField subfieldOverride = new TestRuntimeField("object.subfield");
        TestRuntimeField runtime = new TestRuntimeField("runtime");

        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(List.of(field, concrete, subfield), emptyList(),
            List.of(fieldOverride, runtime, subfieldOverride));
        assertThat(fieldTypeLookup.get("field"), instanceOf(TestRuntimeField.class));
        assertThat(fieldTypeLookup.get("object.subfield"), instanceOf(TestRuntimeField.class));
        assertThat(fieldTypeLookup.get("concrete"), instanceOf(MockFieldMapper.FakeFieldType.class));
        assertThat(fieldTypeLookup.get("runtime"), instanceOf(TestRuntimeField.class));
        assertEquals(4, size(fieldTypeLookup.iterator()));
    }

    public void testRuntimeFieldsSimpleMatchToFullName() {
        MockFieldMapper field1 = new MockFieldMapper("field1");
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField field2 = new TestRuntimeField("field2");
        TestRuntimeField subfield = new TestRuntimeField("object.subfield");

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
        MockFieldMapper field1 = new MockFieldMapper("field1");
        MockFieldMapper concrete = new MockFieldMapper("concrete");
        TestRuntimeField field2 = new TestRuntimeField("field2");
        TestRuntimeField subfield = new TestRuntimeField("object.subfield");

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

    private static int size(Iterator<MappedFieldType> iterator) {
        if (iterator == null) {
            throw new NullPointerException("iterator");
        }
        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        return count;
    }
}
