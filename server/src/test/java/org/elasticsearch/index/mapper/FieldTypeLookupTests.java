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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.emptyList;

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

    public void testDefaultMapping() {
        FieldTypeLookup lookup = new FieldTypeLookup();
        try {
            lookup.copyAndAddAll(MapperService.DEFAULT_MAPPING, emptyList(), emptyList());
            fail();
        } catch (IllegalArgumentException expected) {
            assertEquals("Default mappings should not be added to the lookup", expected.getMessage());
        }
    }

    public void testAddNewField() {
        FieldTypeLookup lookup = new FieldTypeLookup();
        MockFieldMapper f = new MockFieldMapper("foo");
        FieldTypeLookup lookup2 = lookup.copyAndAddAll("type", newList(f), emptyList());
        assertNull(lookup.get("foo"));
        assertNull(lookup.get("bar"));
        assertEquals(f.fieldType(), lookup2.get("foo"));
        assertNull(lookup.get("bar"));
        assertEquals(1, size(lookup2.iterator()));
    }

    public void testAddExistingField() {
        MockFieldMapper f = new MockFieldMapper("foo");
        MockFieldMapper f2 = new MockFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll("type1", newList(f), emptyList());
        FieldTypeLookup lookup2 = lookup.copyAndAddAll("type2", newList(f2), emptyList());

        assertEquals(1, size(lookup2.iterator()));
        assertSame(f.fieldType(), lookup2.get("foo"));
        assertEquals(f2.fieldType(), lookup2.get("foo"));
    }

    public void testMismatchedFieldTypes() {
        FieldMapper f1 = new MockFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll("type", newList(f1), emptyList());

        OtherFakeFieldType ft2 = new OtherFakeFieldType();
        ft2.setName("foo");
        FieldMapper f2 = new MockFieldMapper("foo", ft2);
        try {
            lookup.copyAndAddAll("type2", newList(f2), emptyList());
            fail("expected type mismatch");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("cannot be changed from type [faketype] to [otherfaketype]"));
        }
    }

    public void testConflictingFieldTypes() {
        FieldMapper f1 = new MockFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll("type", newList(f1), emptyList());

        MappedFieldType ft2 = new MockFieldMapper.FakeFieldType();
        ft2.setName("foo");
        ft2.setBoost(2.0f);
        FieldMapper f2 = new MockFieldMapper("foo", ft2);
        lookup.copyAndAddAll("type", newList(f2), emptyList()); // boost is updateable, so ok since we are implicitly updating all types
        lookup.copyAndAddAll("type2", newList(f2), emptyList()); // boost is updateable, so ok if forcing
        // now with a non changeable setting
        MappedFieldType ft3 = new MockFieldMapper.FakeFieldType();
        ft3.setName("foo");
        ft3.setStored(true);
        FieldMapper f3 = new MockFieldMapper("foo", ft3);
        try {
            lookup.copyAndAddAll("type2", newList(f3), emptyList());
            fail("expected conflict");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("has different [store] values"));
        }
    }

    public void testAddFieldAlias() {
        MockFieldMapper field = new MockFieldMapper("foo");
        FieldAliasMapper alias = new FieldAliasMapper("alias", "alias", "foo");

        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll("type", newList(field), newList(alias));

        MappedFieldType aliasType = lookup.get("alias");
        assertEquals(field.fieldType(), aliasType);
    }

    public void testUpdateFieldAlias() {
        // Add an alias 'alias' to the concrete field 'foo'.
        MockFieldMapper.FakeFieldType fieldType1 = new MockFieldMapper.FakeFieldType();
        MockFieldMapper field1 = new MockFieldMapper("foo", fieldType1);
        FieldAliasMapper alias1 = new FieldAliasMapper("alias", "alias", "foo");

        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll("type", newList(field1), newList(alias1));

        // Check that the alias refers to 'foo'.
        MappedFieldType aliasType1 = lookup.get("alias");
        assertEquals(fieldType1, aliasType1);

        // Update the alias to refer to a new concrete field 'bar'.
        MockFieldMapper.FakeFieldType fieldType2 = new MockFieldMapper.FakeFieldType();
        fieldType2.setStored(!fieldType1.stored());
        MockFieldMapper field2 = new MockFieldMapper("bar", fieldType2);

        FieldAliasMapper alias2 = new FieldAliasMapper("alias", "alias", "bar");
        lookup = lookup.copyAndAddAll("type", newList(field2), newList(alias2));

        // Check that the alias now refers to 'bar'.
        MappedFieldType aliasType2 = lookup.get("alias");
        assertEquals(fieldType2, aliasType2);
    }

    public void testUpdateConcreteFieldWithAlias() {
        // Add an alias 'alias' to the concrete field 'foo'.
        FieldAliasMapper alias1 = new FieldAliasMapper("alias", "alias", "foo");
        MockFieldMapper.FakeFieldType fieldType1 = new MockFieldMapper.FakeFieldType();
        fieldType1.setBoost(1.0f);
        MockFieldMapper field1 = new MockFieldMapper("foo", fieldType1);

        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll("type", newList(field1), newList(alias1));

        // Check that the alias maps to this field type.
        MappedFieldType aliasType1 = lookup.get("alias");
        assertEquals(fieldType1, aliasType1);

        // Update the boost for field 'foo'.
        MockFieldMapper.FakeFieldType fieldType2 = new MockFieldMapper.FakeFieldType();
        fieldType2.setBoost(2.0f);
        MockFieldMapper field2 = new MockFieldMapper("foo", fieldType2);
        lookup = lookup.copyAndAddAll("type", newList(field2), emptyList());

        // Check that the alias maps to the new field type.
        MappedFieldType aliasType2 = lookup.get("alias");
        assertEquals(fieldType2, aliasType2);
    }

    public void testAliasThatRefersToAlias() {
        MockFieldMapper field = new MockFieldMapper("foo");
        FieldAliasMapper alias = new FieldAliasMapper("alias", "alias", "foo");
        FieldTypeLookup lookup = new FieldTypeLookup()
            .copyAndAddAll("type", newList(field), newList(alias));

        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid-alias", "invalid-alias", "alias");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> lookup.copyAndAddAll("type", emptyList(), newList(invalidAlias)));
        assertEquals("Invalid [path] value [alias] for field alias [invalid-alias]: an alias" +
            " cannot refer to another alias.", e.getMessage());
    }

    public void testAliasThatRefersToItself() {
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid-alias", "invalid-alias", "invalid-alias");

        FieldTypeLookup lookup = new FieldTypeLookup();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> lookup.copyAndAddAll("type", emptyList(), newList(invalidAlias)));
        assertEquals("Invalid [path] value [invalid-alias] for field alias [invalid-alias]: an alias" +
            " cannot refer to itself.", e.getMessage());
    }

    public void testAliasWithNonExistentPath() {
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid-alias", "invalid-alias", "non-existent");

        FieldTypeLookup lookup = new FieldTypeLookup();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> lookup.copyAndAddAll("type", emptyList(), newList(invalidAlias)));
        assertEquals("Invalid [path] value [non-existent] for field alias [invalid-alias]: an alias" +
            " must refer to an existing field in the mappings.", e.getMessage());
    }

    public void testAddAliasWithPreexistingField() {
        MockFieldMapper field = new MockFieldMapper("field");
        FieldTypeLookup lookup = new FieldTypeLookup()
            .copyAndAddAll("type", newList(field), emptyList());

        MockFieldMapper invalidField = new MockFieldMapper("invalid");
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid", "invalid", "field");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> lookup.copyAndAddAll("type", newList(invalidField), newList(invalidAlias)));
        assertEquals("The name for field alias [invalid] has already been used to define a concrete field.",
            e.getMessage());
    }

    public void testAddFieldWithPreexistingAlias() {
        MockFieldMapper field = new MockFieldMapper("field");
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid", "invalid", "field");

        FieldTypeLookup lookup = new FieldTypeLookup()
            .copyAndAddAll("type", newList(field), newList(invalidAlias));

        MockFieldMapper invalidField = new MockFieldMapper("invalid");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> lookup.copyAndAddAll("type", newList(invalidField), emptyList()));
        assertEquals("The name for field [invalid] has already been used to define a field alias.",
            e.getMessage());
    }

    public void testSimpleMatchToFullName() {
        MockFieldMapper field1 = new MockFieldMapper("foo");
        MockFieldMapper field2 = new MockFieldMapper("bar");

        FieldAliasMapper alias1 = new FieldAliasMapper("food", "food", "foo");
        FieldAliasMapper alias2 = new FieldAliasMapper("barometer", "barometer", "bar");

        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll("type",
            newList(field1, field2),
            newList(alias1, alias2));

        Collection<String> names = lookup.simpleMatchToFullName("b*");

        assertFalse(names.contains("foo"));
        assertFalse(names.contains("food"));

        assertTrue(names.contains("bar"));
        assertTrue(names.contains("barometer"));
    }

    public void testIteratorImmutable() {
        MockFieldMapper f1 = new MockFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll("type", newList(f1), emptyList());

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

    private static List<FieldMapper> newList(FieldMapper... mapper) {
        return Arrays.asList(mapper);
    }

    private static List<FieldAliasMapper> newList(FieldAliasMapper... mapper) {
        return Arrays.asList(mapper);
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

    static class OtherFakeFieldType extends TermBasedFieldType {
        OtherFakeFieldType() {
        }

        protected OtherFakeFieldType(OtherFakeFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new OtherFakeFieldType(this);
        }

        @Override
        public String typeName() {
            return "otherfaketype";
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }
    }
}
