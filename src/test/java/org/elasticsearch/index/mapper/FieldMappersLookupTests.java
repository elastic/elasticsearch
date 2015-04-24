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
import com.google.common.collect.Lists;
import org.apache.lucene.document.FieldType;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class FieldMappersLookupTests extends ElasticsearchTestCase {

    public void testEmpty() {
        FieldMappersLookup lookup = new FieldMappersLookup();
        assertNull(lookup.fullName("foo"));
        assertNull(lookup.indexName("foo"));
        List<String> names = lookup.simpleMatchToFullName("foo");
        assertNotNull(names);
        assertTrue(names.isEmpty());
        names = lookup.simpleMatchToFullName("foo");
        assertNotNull(names);
        assertTrue(names.isEmpty());
        assertNull(lookup.smartName("foo"));
        assertNull(lookup.smartNameFieldMapper("foo"));
        assertNull(lookup.get("foo"));
        Iterator<FieldMapper<?>> itr = lookup.iterator();
        assertNotNull(itr);
        assertFalse(itr.hasNext());
    }

    public void testNewField() {
        FieldMappersLookup lookup = new FieldMappersLookup();
        FakeFieldMapper f = new FakeFieldMapper("foo", "bar");
        FieldMappersLookup lookup2 = lookup.copyAndAddAll(newList(f));
        assertNull(lookup.fullName("foo"));
        assertNull(lookup.indexName("bar"));

        FieldMappers mappers = lookup2.fullName("foo");
        assertNotNull(mappers);
        assertEquals(1, mappers.mappers().size());
        assertEquals(f, mappers.mapper());
        mappers = lookup2.indexName("bar");
        assertNotNull(mappers);
        assertEquals(1, mappers.mappers().size());
        assertEquals(f, mappers.mapper());
        assertEquals(1, Iterators.size(lookup2.iterator()));
    }

    public void testExtendField() {
        FieldMappersLookup lookup = new FieldMappersLookup();
        FakeFieldMapper f = new FakeFieldMapper("foo", "bar");
        FakeFieldMapper other = new FakeFieldMapper("blah", "blah");
        lookup = lookup.copyAndAddAll(newList(f, other));
        FakeFieldMapper f2 = new FakeFieldMapper("foo", "bar");
        FieldMappersLookup lookup2 = lookup.copyAndAddAll(newList(f2));

        FieldMappers mappers = lookup2.fullName("foo");
        assertNotNull(mappers);
        assertEquals(2, mappers.mappers().size());

        mappers = lookup2.indexName("bar");
        assertNotNull(mappers);
        assertEquals(2, mappers.mappers().size());
        assertEquals(3, Iterators.size(lookup2.iterator()));
    }

    public void testIndexName() {
        FakeFieldMapper f1 = new FakeFieldMapper("foo", "foo");
        FieldMappersLookup lookup = new FieldMappersLookup();
        lookup = lookup.copyAndAddAll(newList(f1));

        FieldMappers mappers = lookup.indexName("foo");
        assertNotNull(mappers);
        assertEquals(1, mappers.mappers().size());
        assertEquals(f1, mappers.mapper());
    }

    public void testSimpleMatchIndexNames() {
        FakeFieldMapper f1 = new FakeFieldMapper("foo", "baz");
        FakeFieldMapper f2 = new FakeFieldMapper("bar", "boo");
        FieldMappersLookup lookup = new FieldMappersLookup();
        lookup = lookup.copyAndAddAll(newList(f1, f2));
        List<String> names = lookup.simpleMatchToIndexNames("b*");
        assertTrue(names.contains("baz"));
        assertTrue(names.contains("boo"));
    }

    public void testSimpleMatchFullNames() {
        FakeFieldMapper f1 = new FakeFieldMapper("foo", "baz");
        FakeFieldMapper f2 = new FakeFieldMapper("bar", "boo");
        FieldMappersLookup lookup = new FieldMappersLookup();
        lookup = lookup.copyAndAddAll(newList(f1, f2));
        List<String> names = lookup.simpleMatchToFullName("b*");
        assertTrue(names.contains("foo"));
        assertTrue(names.contains("bar"));
    }

    public void testSmartName() {
        FakeFieldMapper f1 = new FakeFieldMapper("foo", "realfoo");
        FakeFieldMapper f2 = new FakeFieldMapper("foo", "realbar");
        FakeFieldMapper f3 = new FakeFieldMapper("baz", "realfoo");
        FieldMappersLookup lookup = new FieldMappersLookup();
        lookup = lookup.copyAndAddAll(newList(f1, f2, f3));

        assertNotNull(lookup.smartName("foo"));
        assertEquals(2, lookup.smartName("foo").mappers().size());
        assertNotNull(lookup.smartName("realfoo"));
        assertEquals(f1, lookup.smartNameFieldMapper("foo"));
        assertEquals(f2, lookup.smartNameFieldMapper("realbar"));
    }

    public void testIteratorImmutable() {
        FakeFieldMapper f1 = new FakeFieldMapper("foo", "bar");
        FieldMappersLookup lookup = new FieldMappersLookup();
        lookup = lookup.copyAndAddAll(newList(f1));

        try {
            Iterator<FieldMapper<?>> itr = lookup.iterator();
            assertTrue(itr.hasNext());
            assertEquals(f1, itr.next());
            itr.remove();
            fail("remove should have failed");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    public void testGetMapper() {
        FakeFieldMapper f1 = new FakeFieldMapper("foo", "bar");
        FieldMappersLookup lookup = new FieldMappersLookup();
        lookup = lookup.copyAndAddAll(newList(f1));

        assertEquals(f1, lookup.get("foo"));
        assertNull(lookup.get("bar")); // get is only by full name
        FakeFieldMapper f2 = new FakeFieldMapper("foo", "foo");
        lookup = lookup.copyAndAddAll(newList(f2));
        try {
            lookup.get("foo");
            fail("get should have enforced foo is unique");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    static List<FieldMapper<?>> newList(FieldMapper<?>... mapper) {
        return Lists.newArrayList(mapper);
    }

    // this sucks how much must be overriden just do get a dummy field mapper...
    static class FakeFieldMapper extends AbstractFieldMapper<String> {
        static Settings dummySettings = ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        public FakeFieldMapper(String fullName, String indexName) {
            super(new Names(fullName, indexName, indexName, fullName), 1.0f, AbstractFieldMapper.Defaults.FIELD_TYPE, null, null, null, null, null, null, dummySettings, null, null);
        }
        @Override
        public FieldType defaultFieldType() { return null; }
        @Override
        public FieldDataType defaultFieldDataType() { return null; }
        @Override
        protected String contentType() { return null; }
        @Override
        protected void parseCreateField(ParseContext context, List list) throws IOException {}
        @Override
        public String value(Object value) { return null; }
    }
}
