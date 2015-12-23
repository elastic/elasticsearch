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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class FieldTypeLookupTests extends ESTestCase {

    public void testEmpty() {
        FieldTypeLookup lookup = new FieldTypeLookup();
        assertNull(lookup.get("foo"));
        assertEquals(Collections.emptySet(), lookup.getTypes("foo"));
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
            lookup.copyAndAddAll(MapperService.DEFAULT_MAPPING, Collections.emptyList(), randomBoolean());
            fail();
        } catch (IllegalArgumentException expected) {
            assertEquals("Default mappings should not be added to the lookup", expected.getMessage());
        }
    }

    public void testAddNewField() {
        FieldTypeLookup lookup = new FieldTypeLookup();
        FakeFieldMapper f = new FakeFieldMapper("foo");
        FieldTypeLookup lookup2 = lookup.copyAndAddAll("type", newList(f), randomBoolean());
        assertNull(lookup.get("foo"));
        assertNull(lookup.get("bar"));
        assertEquals(f.fieldType(), lookup2.get("foo"));
        assertNull(lookup.get("bar"));
        assertEquals(Collections.emptySet(), lookup.getTypes("foo"));
        assertEquals(Collections.emptySet(), lookup.getTypes("bar"));
        assertEquals(Collections.singleton("type"), lookup2.getTypes("foo"));
        assertEquals(Collections.emptySet(), lookup2.getTypes("bar"));
        assertEquals(1, size(lookup2.iterator()));
    }

    public void testAddExistingField() {
        FakeFieldMapper f = new FakeFieldMapper("foo");
        FakeFieldMapper f2 = new FakeFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll("type1", newList(f), randomBoolean());
        FieldTypeLookup lookup2 = lookup.copyAndAddAll("type2", newList(f2), randomBoolean());

        assertSame(f2.fieldType(), lookup2.get("foo"));
        assertEquals(1, size(lookup2.iterator()));
    }

    public void testAddExistingIndexName() {
        FakeFieldMapper f = new FakeFieldMapper("foo");
        FakeFieldMapper f2 = new FakeFieldMapper("bar");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll("type1", newList(f), randomBoolean());
        FieldTypeLookup lookup2 = lookup.copyAndAddAll("type2", newList(f2), randomBoolean());

        assertSame(f.fieldType(), lookup2.get("foo"));
        assertSame(f2.fieldType(), lookup2.get("bar"));
        assertEquals(2, size(lookup2.iterator()));
    }

    public void testAddExistingFullName() {
        FakeFieldMapper f = new FakeFieldMapper("foo");
        FakeFieldMapper f2 = new FakeFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup();
        try {
            lookup.copyAndAddAll("type2", newList(f2), randomBoolean());
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("mapper [foo] has different [index_name]"));
        }
    }

    public void testCheckCompatibilityMismatchedTypes() {
        FieldMapper f1 = new FakeFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll("type", newList(f1), randomBoolean());

        MappedFieldType ft2 = FakeFieldMapper.makeOtherFieldType("foo");
        FieldMapper f2 = new FakeFieldMapper("foo", ft2);
        try {
            lookup.copyAndAddAll("type2", newList(f2), false);
            fail("expected type mismatch");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("cannot be changed from type [faketype] to [otherfaketype]"));
        }
        // fails even if updateAllTypes == true
        try {
            lookup.copyAndAddAll("type2", newList(f2), true);
            fail("expected type mismatch");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("cannot be changed from type [faketype] to [otherfaketype]"));
        }
    }

    public void testCheckCompatibilityConflict() {
        FieldMapper f1 = new FakeFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll("type", newList(f1), randomBoolean());

        MappedFieldType ft2 = FakeFieldMapper.makeFieldType("foo");
        ft2.setBoost(2.0f);
        FieldMapper f2 = new FakeFieldMapper("foo", ft2);
        try {
            // different type
            lookup.copyAndAddAll("type2", newList(f2), false);
            fail("expected conflict");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("to update [boost] across all types"));
        }
        lookup.copyAndAddAll("type", newList(f2), false); // boost is updateable, so ok since we are implicitly updating all types
        lookup.copyAndAddAll("type2", newList(f2), true); // boost is updateable, so ok if forcing
        // now with a non changeable setting
        MappedFieldType ft3 = FakeFieldMapper.makeFieldType("foo");
        ft3.setStored(true);
        FieldMapper f3 = new FakeFieldMapper("foo", ft3);
        try {
            lookup.copyAndAddAll("type2", newList(f3), false);
            fail("expected conflict");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("has different [store] values"));
        }
        // even with updateAllTypes == true, incompatible
        try {
            lookup.copyAndAddAll("type2", newList(f3), true);
            fail("expected conflict");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("has different [store] values"));
        }
    }

    public void testSimpleMatchFullNames() {
        FakeFieldMapper f1 = new FakeFieldMapper("foo");
        FakeFieldMapper f2 = new FakeFieldMapper("bar");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll("type", newList(f1, f2), randomBoolean());
        Collection<String> names = lookup.simpleMatchToFullName("b*");
        assertFalse(names.contains("foo"));
        assertTrue(names.contains("bar"));
    }

    public void testIteratorImmutable() {
        FakeFieldMapper f1 = new FakeFieldMapper("foo");
        FieldTypeLookup lookup = new FieldTypeLookup();
        lookup = lookup.copyAndAddAll("type", newList(f1), randomBoolean());

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
        public FakeFieldMapper(String fullName) {
            super(fullName, makeFieldType(fullName), makeFieldType(fullName), dummySettings, null, null);
        }
        public FakeFieldMapper(String fullName, MappedFieldType fieldType) {
            super(fullName, fieldType, fieldType, dummySettings, null, null);
        }
        static MappedFieldType makeFieldType(String fullName) {
            FakeFieldType fieldType = new FakeFieldType();
            fieldType.setName(fullName);
            return fieldType;
        }
        static MappedFieldType makeOtherFieldType(String fullName) {
            OtherFakeFieldType fieldType = new OtherFakeFieldType();
            fieldType.setName(fullName);
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
