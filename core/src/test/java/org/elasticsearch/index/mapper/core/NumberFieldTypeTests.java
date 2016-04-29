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

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexOptions;
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
}
