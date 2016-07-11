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

package org.elasticsearch.index.mapper.core;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.HalfFloatPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.index.mapper.core.NumberFieldMapper.NumberType;
import org.junit.Before;

import java.io.IOException;

public class NumberFieldTypeTests extends FieldTypeTestCase {

    NumberType type;

    @Before
    public void pickType() {
        type = RandomPicks.randomFrom(random(), NumberFieldMapper.NumberType.values());
    }

    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new NumberFieldMapper.NumberFieldType(type);
    }

    public void testIsFieldWithinQuery() throws IOException {
        MappedFieldType ft = createDefaultFieldType();
        // current impl ignores args and should always return INTERSECTS
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(null, randomDouble(), randomDouble(),
                randomBoolean(), randomBoolean(), null, null));
    }

    public void testTermQuery() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(LongPoint.newExactQuery("field", 42), ft.termQuery("42", null));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.termQuery("42", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testRangeQuery() {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(LongPoint.newRangeQuery("field", 1, 3), ft.rangeQuery("1", "3", true, true));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.rangeQuery("1", "3", true, true));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testConversions() {
        assertEquals((byte) 3, NumberType.BYTE.parse(3d));
        assertEquals((short) 3, NumberType.SHORT.parse(3d));
        assertEquals(3, NumberType.INTEGER.parse(3d));
        assertEquals(3L, NumberType.LONG.parse(3d));
        assertEquals(3f, NumberType.HALF_FLOAT.parse(3d));
        assertEquals(3f, NumberType.FLOAT.parse(3d));
        assertEquals(3d, NumberType.DOUBLE.parse(3d));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> NumberType.BYTE.parse(3.5));
        assertEquals("Value [3.5] has a decimal part", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> NumberType.SHORT.parse(3.5));
        assertEquals("Value [3.5] has a decimal part", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> NumberType.INTEGER.parse(3.5));
        assertEquals("Value [3.5] has a decimal part", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> NumberType.LONG.parse(3.5));
        assertEquals("Value [3.5] has a decimal part", e.getMessage());
        assertEquals(3.5f, NumberType.FLOAT.parse(3.5));
        assertEquals(3.5d, NumberType.DOUBLE.parse(3.5));

        e = expectThrows(IllegalArgumentException.class, () -> NumberType.BYTE.parse(128));
        assertEquals("Value [128] is out of range for a byte", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> NumberType.SHORT.parse(65536));
        assertEquals("Value [65536] is out of range for a short", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> NumberType.INTEGER.parse(2147483648L));
        assertEquals("Value [2147483648] is out of range for an integer", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> NumberType.LONG.parse(10000000000000000000d));
        assertEquals("Value [1.0E19] is out of range for a long", e.getMessage());
        assertEquals(1.1f, NumberType.HALF_FLOAT.parse(1.1));
        assertEquals(1.1f, NumberType.FLOAT.parse(1.1));
        assertEquals(1.1d, NumberType.DOUBLE.parse(1.1));
    }

    public void testHalfFloatRange() throws IOException {
        // make sure the accuracy loss of half floats only occurs at index time
        // this test checks that searching half floats yields the same results as
        // searching floats that are rounded to the closest half float
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        final int numDocs = 10000;
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            float value = (randomFloat() * 2 - 1) * 70000;
            float rounded = HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(value));
            doc.add(new HalfFloatPoint("half_float", value));
            doc.add(new FloatPoint("float", rounded));
            w.addDocument(doc);
        }
        final DirectoryReader reader = DirectoryReader.open(w);
        w.close();
        IndexSearcher searcher = newSearcher(reader);
        final int numQueries = 1000;
        for (int i = 0; i < numQueries; ++i) {
            float l = (randomFloat() * 2 - 1) * 70000;
            float u = (randomFloat() * 2 - 1) * 70000;
            boolean includeLower = randomBoolean();
            boolean includeUpper = randomBoolean();
            Query floatQ = NumberFieldMapper.NumberType.FLOAT.rangeQuery("float", l, u, includeLower, includeUpper);
            Query halfFloatQ = NumberFieldMapper.NumberType.HALF_FLOAT.rangeQuery("half_float", l, u, includeLower, includeUpper);
            assertEquals(searcher.count(floatQ), searcher.count(halfFloatQ));
        }
        IOUtils.close(reader, dir);
    }
}
