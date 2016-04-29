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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.junit.Before;

public class BooleanFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new BooleanFieldMapper.BooleanFieldType();
    }

    @Before
    public void setupProperties() {
        setDummyNullValue(true);
    }

    public void testValueFormat() {
        MappedFieldType ft = createDefaultFieldType();
        assertEquals("false", ft.docValueFormat(null, null).format(0));
        assertEquals("true", ft.docValueFormat(null, null).format(1));
    }

    public void testValueForSearch() {
        MappedFieldType ft = createDefaultFieldType();
        assertEquals(true, ft.valueForSearch("T"));
        assertEquals(false, ft.valueForSearch("F"));
        expectThrows(IllegalArgumentException.class, () -> ft.valueForSearch(0));
        expectThrows(IllegalArgumentException.class, () -> ft.valueForSearch("true"));
        expectThrows(IllegalArgumentException.class, () -> ft.valueForSearch("G"));
    }

    public void testTermQuery() {
        MappedFieldType ft = createDefaultFieldType();
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(new TermQuery(new Term("field", "T")), ft.termQuery("true", null));
        assertEquals(new TermQuery(new Term("field", "F")), ft.termQuery("false", null));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.termQuery("true", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }
}
