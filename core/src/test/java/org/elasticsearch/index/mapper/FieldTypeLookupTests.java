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

import com.google.common.collect.Iterators;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class FieldTypeLookupTests extends ESTestCase {

    public void testEmpty() {
        FieldTypeLookup lookup = new FieldTypeLookup();
        assertNull(lookup.get("foo"));
        assertNull(lookup.getByIndexName("foo"));
        Collection<String> names = lookup.simpleMatchToFullName("foo");
        assertNotNull(names);
        assertTrue(names.isEmpty());
        names = lookup.simpleMatchToIndexNames("foo");
        assertNotNull(names);
        assertTrue(names.isEmpty());
        Iterator<MappedFieldType> itr = lookup.iterator();
        assertNotNull(itr);
        assertFalse(itr.hasNext());
    }

    public void testAddNewField() {
        FieldTypeLookup lookup = new FieldTypeLookup();
        FakeFieldMapper f = new FakeFieldMapper("foo", "bar");
        FieldTypeLookup lookup2 = lookup.copyAndAddAll(newList(f));
        assertNull(lookup.get("foo"));
        assertNull(lookup.get("bar"));
        assertNull(lookup.getByIndexName("foo"));
        assertNull(lookup.getByIndexName("bar"));
        assertEquals(f.fieldType(), lookup2.get("foo"));
        assertNull(lookup.get("bar"));
        assertEquals(f.fieldType(), lookup2.getByIndexName("bar"));
        assertNull(lookup.getByIndexName("foo"));
        assertEquals(1, Iterators.size(lookup2.iterator()));
    }

    public void testAddExistingField() {
        FakeFieldMapper f = new FakeFieldMapper("foo", "foo");
        MappedFieldType originalFieldType = f.fieldType();
        FakeFieldMapper f2 = new FakeFieldMapper("foo", "foo");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll(newList(f));
        FieldTypeLookup lookup2 = lookup.copyAndAddAll(newList(f2));

        assertNotSame(originalFieldType, f.fieldType());
        assertSame(f.fieldType(), f2.fieldType());
        assertSame(f.fieldType(), lookup2.get("foo"));
        assertSame(f.fieldType(), lookup2.getByIndexName("foo"));
        assertEquals(1, Iterators.size(lookup2.iterator()));
    }

    public void testAddExistingIndexName() {
        FakeFieldMapper f = new FakeFieldMapper("foo", "foo");
        FakeFieldMapper f2 = new FakeFieldMapper("bar", "foo");
        MappedFieldType originalFieldType = f.fieldType();
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll(newList(f));
        FieldTypeLookup lookup2 = lookup.copyAndAddAll(newList(f2));

        assertNotSame(originalFieldType, f.fieldType());
        assertSame(f.fieldType(), f2.fieldType());
        assertSame(f.fieldType(), lookup2.get("foo"));
        assertSame(f.fieldType(), lookup2.get("bar"));
        assertSame(f.fieldType(), lookup2.getByIndexName("foo"));
        assertEquals(2, Iterators.size(lookup2.iterator()));
    }

    public void testAddExistingFullName() {
        FakeFieldMapper f = new FakeFieldMapper("foo", "foo");
        FakeFieldMapper f2 = new FakeFieldMapper("foo", "bar");
        MappedFieldType originalFieldType = f.fieldType();
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll(newList(f));
        FieldTypeLookup lookup2 = lookup.copyAndAddAll(newList(f2));

        assertNotSame(originalFieldType, f.fieldType());
        assertSame(f.fieldType(), f2.fieldType());
        assertSame(f.fieldType(), lookup2.get("foo"));
        assertSame(f.fieldType(), lookup2.getByIndexName("foo"));
        assertSame(f.fieldType(), lookup2.getByIndexName("bar"));
        assertEquals(1, Iterators.size(lookup2.iterator()));
    }

    public void testAddExistingBridgeName() {
        FakeFieldMapper f = new FakeFieldMapper("foo", "foo");
        FakeFieldMapper f2 = new FakeFieldMapper("bar", "bar");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll(newList(f, f2));

        try {
            FakeFieldMapper f3 = new FakeFieldMapper("foo", "bar");
            lookup.copyAndAddAll(newList(f3));
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("insane mappings"));
        }

        try {
            FakeFieldMapper f3 = new FakeFieldMapper("bar", "foo");
            lookup.copyAndAddAll(newList(f3));
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("insane mappings"));
        }
    }

    public void testCheckCompatibilityNewField() {
        FakeFieldMapper f1 = new FakeFieldMapper("foo", "bar");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup.checkCompatibility(newList(f1), false);
    }

    public void testCheckCompatibilityMismatchedTypes() {
        FieldMapper f1 = new FakeFieldMapper("foo", "bar");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll(newList(f1));

        MappedFieldType ft2 = FakeFieldMapper.makeOtherFieldType("foo", "foo");
        FieldMapper f2 = new FakeFieldMapper("foo", ft2);
        try {
            lookup.checkCompatibility(newList(f2), false);
            fail("expected type mismatch");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("cannot be changed from type [faketype] to [otherfaketype]"));
        }
        // fails even if updateAllTypes == true
        try {
            lookup.checkCompatibility(newList(f2), true);
            fail("expected type mismatch");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("cannot be changed from type [faketype] to [otherfaketype]"));
        }
    }

    public void testCheckCompatibilityConflict() {
        FieldMapper f1 = new FakeFieldMapper("foo", "bar");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll(newList(f1));

        MappedFieldType ft2 = FakeFieldMapper.makeFieldType("foo", "bar");
        ft2.setBoost(2.0f);
        FieldMapper f2 = new FakeFieldMapper("foo", ft2);
        try {
            lookup.checkCompatibility(newList(f2), false);
            fail("expected conflict");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("to update [boost] across all types"));
        }
        lookup.checkCompatibility(newList(f2), true); // boost is updateable, so ok if forcing
        // now with a non changeable setting
        MappedFieldType ft3 = FakeFieldMapper.makeFieldType("foo", "bar");
        ft3.setStored(true);
        FieldMapper f3 = new FakeFieldMapper("foo", ft3);
        try {
            lookup.checkCompatibility(newList(f3), false);
            fail("expected conflict");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("has different [store] values"));
        }
        // even with updateAllTypes == true, incompatible
        try {
            lookup.checkCompatibility(newList(f3), true);
            fail("expected conflict");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("has different [store] values"));
        }
    }

    public void testSimpleMatchIndexNames() {
        FakeFieldMapper f1 = new FakeFieldMapper("foo", "baz");
        FakeFieldMapper f2 = new FakeFieldMapper("bar", "boo");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll(newList(f1, f2));
        Collection<String> names = lookup.simpleMatchToIndexNames("b*");
        assertTrue(names.contains("baz"));
        assertTrue(names.contains("boo"));
    }

    public void testSimpleMatchFullNames() {
        FakeFieldMapper f1 = new FakeFieldMapper("foo", "baz");
        FakeFieldMapper f2 = new FakeFieldMapper("bar", "boo");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll(newList(f1, f2));
        Collection<String> names = lookup.simpleMatchToFullName("b*");
        assertTrue(names.contains("foo"));
        assertTrue(names.contains("bar"));
    }

    public void testIteratorImmutable() {
        FakeFieldMapper f1 = new FakeFieldMapper("foo", "bar");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll(newList(f1));

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

    static List<FieldMapper> newList(FieldMapper... mapper) {
        return Arrays.asList(mapper);
    }

    // this sucks how much must be overridden just do get a dummy field mapper...
    static class FakeFieldMapper extends FieldMapper {
        static Settings dummySettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        public FakeFieldMapper(String fullName, String indexName) {
            super(fullName, makeFieldType(fullName, indexName), makeFieldType(fullName, indexName), dummySettings, null, null);
        }
        public FakeFieldMapper(String fullName, MappedFieldType fieldType) {
            super(fullName, fieldType, fieldType, dummySettings, null, null);
        }
        static MappedFieldType makeFieldType(String fullName, String indexName) {
            FakeFieldType fieldType = new FakeFieldType();
            fieldType.setNames(new MappedFieldType.Names(indexName, indexName, fullName));
            return fieldType;
        }
        static MappedFieldType makeOtherFieldType(String fullName, String indexName) {
            OtherFakeFieldType fieldType = new OtherFakeFieldType();
            fieldType.setNames(new MappedFieldType.Names(indexName, indexName, fullName));
            return fieldType;
        }
        static class FakeFieldType extends MappedFieldType {
            public FakeFieldType() {}
            protected FakeFieldType(FakeFieldType ref) {
                super(ref);
            }
            @Override
            public MappedFieldType clone() {
                return new FakeFieldType(this);
            }
            @Override
            public String typeName() {
                return "faketype";
            }
        }
        static class OtherFakeFieldType extends MappedFieldType {
            public OtherFakeFieldType() {}
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
        }
        @Override
        protected String contentType() { return null; }
        @Override
        protected void parseCreateField(ParseContext context, List list) throws IOException {}
    }
}
