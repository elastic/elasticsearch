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
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class FieldTypeLookupTests extends ESTestCase {

    public void testEmpty() {
        FieldTypeLookup lookup = new FieldTypeLookup();
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
        FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(f), emptyList());
        assertNull(lookup.get("bar"));
        assertEquals(f.fieldType(), lookup.get("foo"));
        assertEquals(1, size(lookup.iterator()));
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
        Mapper.BuilderContext context = new Mapper.BuilderContext(
            MockFieldMapper.DEFAULT_SETTINGS, new ContentPath());

        MockFieldMapper field = new MockFieldMapper.Builder("field")
            .addMultiField(new MockFieldMapper.Builder("field.subfield1"))
            .addMultiField(new MockFieldMapper.Builder("field.subfield2"))
            .build(context);

        FieldTypeLookup lookup = new FieldTypeLookup(singletonList(field), emptyList());

        assertEquals(Set.of("field"), lookup.sourcePaths("field"));
        assertEquals(Set.of("field"), lookup.sourcePaths("field.subfield1"));
        assertEquals(Set.of("field"), lookup.sourcePaths("field.subfield2"));
    }

    public void testSourcePathWithAliases() {
        Mapper.BuilderContext context = new Mapper.BuilderContext(
            MockFieldMapper.DEFAULT_SETTINGS, new ContentPath());

        MockFieldMapper field = new MockFieldMapper.Builder("field")
            .addMultiField(new MockFieldMapper.Builder("field.subfield"))
            .build(context);

        FieldAliasMapper alias1 = new FieldAliasMapper("alias1", "alias1", "field");
        FieldAliasMapper alias2 = new FieldAliasMapper("alias2", "alias2", "field.subfield");

        FieldTypeLookup lookup = new FieldTypeLookup(Arrays.asList(field), Arrays.asList(alias1, alias2));

        assertEquals(Set.of("field"), lookup.sourcePaths("alias1"));
        assertEquals(Set.of("field"), lookup.sourcePaths("alias2"));
    }

    public void testSourcePathsWithCopyTo() {
        Mapper.BuilderContext context = new Mapper.BuilderContext(
            MockFieldMapper.DEFAULT_SETTINGS, new ContentPath());

        MockFieldMapper field = new MockFieldMapper.Builder("field")
            .addMultiField(new MockFieldMapper.Builder("field.subfield1"))
            .build(context);

        MockFieldMapper otherField = new MockFieldMapper.Builder("other_field")
            .copyTo(new FieldMapper.CopyTo.Builder()
                .add("field")
                .build())
            .build(context);

        FieldTypeLookup lookup = new FieldTypeLookup(Arrays.asList(field, otherField), emptyList());

        assertEquals(Set.of("other_field", "field"), lookup.sourcePaths("field"));
        assertEquals(Set.of("other_field", "field"), lookup.sourcePaths("field.subfield1"));
    }

    public void testIteratorImmutable() {
        MockFieldMapper f1 = new MockFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup(Collections.singletonList(f1), emptyList());

        try {
            Iterator<MappedFieldType> itr = lookup.iterator();
            assertTrue(itr.hasNext());
            assertEquals(f1.fieldType(), itr.next());
            itr.remove();
            fail("remove should have failed");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    private int size(Iterator<MappedFieldType> iterator) {
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
