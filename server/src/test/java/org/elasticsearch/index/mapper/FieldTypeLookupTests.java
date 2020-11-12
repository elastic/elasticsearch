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
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;

public class FieldTypeLookupTests extends ESTestCase {

    public void testEmpty() {
        FieldTypeLookup lookup = new FieldTypeLookup(Collections.emptyList(), Collections.emptyList());
        assertNull(lookup.get("foo"));
        Collection<String> names = lookup.simpleMatchToFullName("foo");
        assertNotNull(names);
        assertTrue(names.isEmpty());
        assertEquals(0, size(lookup.filter(ft -> true)));
    }

    public void testFilter() {
        Collection<FieldMapper> fieldMappers = List.of(new MockFieldMapper("field"), new MockFieldMapper("test"));
        Collection<FieldAliasMapper> fieldAliases = singletonList(new FieldAliasMapper("alias", "alias", "test"));
        FieldTypeLookup fieldTypeLookup = new FieldTypeLookup(fieldMappers, fieldAliases);
        Iterable<MappedFieldType> allFieldTypes = fieldTypeLookup.filter(ft -> true);
        assertEquals(2, size(allFieldTypes));
        for (MappedFieldType allFieldType : allFieldTypes) {
            assertThat(allFieldType, instanceOf(MockFieldMapper.FakeFieldType.class));
        }
        assertEquals(0, size(fieldTypeLookup.filter(ft -> false)));
        Iterable<MappedFieldType> fieldIterable = fieldTypeLookup.filter(ft -> ft.name().equals("field"));
        assertEquals(1, size(fieldIterable));
        MappedFieldType field = fieldIterable.iterator().next();
        assertEquals("field", field.name());
        assertThat(field, instanceOf(MockFieldMapper.FakeFieldType.class));
    }

    public void testAddNewField() {
        MockFieldMapper f = new MockFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(f), emptyList());
        assertNull(lookup.get("bar"));
        assertEquals(f.fieldType(), lookup.get("foo"));
        assertEquals(1, size(lookup.filter(ft -> true)));
    }

    public void testAddFieldAlias() {
        MockFieldMapper field = new MockFieldMapper("foo");
        FieldAliasMapper alias = new FieldAliasMapper("alias", "alias", "foo");

        FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(field), Collections.singletonList(alias));

        MappedFieldType aliasType = lookup.get("alias");
        assertEquals(field.fieldType(), aliasType);
    }

    public void testSimpleMatchToFullName() {
        MockFieldMapper field1 = new MockFieldMapper("foo");
        MockFieldMapper field2 = new MockFieldMapper("bar");

        FieldAliasMapper alias1 = new FieldAliasMapper("food", "food", "foo");
        FieldAliasMapper alias2 = new FieldAliasMapper("barometer", "barometer", "bar");

        FieldTypeLookup lookup = new FieldTypeLookup(Arrays.asList(field1, field2), Arrays.asList(alias1, alias2));

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

        FieldTypeLookup lookup = new FieldTypeLookup(singletonList(field), emptyList());

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

        FieldTypeLookup lookup = new FieldTypeLookup(Arrays.asList(field, otherField), emptyList());

        assertEquals(Set.of("other_field", "field"), lookup.sourcePaths("field"));
        assertEquals(Set.of("other_field", "field"), lookup.sourcePaths("field.subfield1"));
    }

    private int size(Iterable<MappedFieldType> iterable) {
        int count = 0;
        for (MappedFieldType fieldType : iterable) {
            count++;
        }
        return count;
    }
}
