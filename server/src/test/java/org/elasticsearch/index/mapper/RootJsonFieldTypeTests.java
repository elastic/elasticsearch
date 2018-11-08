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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.JsonFieldMapper.RootJsonFieldType;
import org.junit.Before;

public class RootJsonFieldTypeTests extends FieldTypeTestCase {

    @Before
    public void setupProperties() {
        addModifier(new Modifier("split_queries_on_whitespace", true) {
            @Override
            public void modify(MappedFieldType type) {
                RootJsonFieldType ft = (RootJsonFieldType) type;
                ft.setSplitQueriesOnWhitespace(!ft.splitQueriesOnWhitespace());
            }
        });
    }

    @Override
    protected RootJsonFieldType createDefaultFieldType() {
        return new RootJsonFieldType();
    }

    public void testValueForDisplay() {
        RootJsonFieldType ft = createDefaultFieldType();

        String fieldValue = "{ \"key\": \"value\" }";
        BytesRef storedValue = new BytesRef(fieldValue);
        assertEquals(fieldValue, ft.valueForDisplay(storedValue));
    }

    public void testTermQuery() {
        RootJsonFieldType ft = createDefaultFieldType();
        ft.setName("field");

        Query expected = new TermQuery(new Term("field", "value"));
        assertEquals(expected, ft.termQuery("value", null));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> ft.termQuery("field", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testExistsQuery() {
        RootJsonFieldType ft = new RootJsonFieldType();
        ft.setName("field");

        Query expected = new TermQuery(new Term(FieldNamesFieldMapper.NAME, new BytesRef("field")));
        assertEquals(expected, ft.existsQuery(null));
    }

    public void testFuzzyQuery() {
        RootJsonFieldType ft = createDefaultFieldType();
        ft.setName("field");

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
            () -> ft.fuzzyQuery("valuee", Fuzziness.fromEdits(2), 1, 50, true));
        assertEquals("[fuzzy] queries are not currently supported on [json] fields.", e.getMessage());
    }

    public void testRangeQuery() {
        RootJsonFieldType ft = createDefaultFieldType();
        ft.setName("field");

        TermRangeQuery expected = new TermRangeQuery("field",
            new BytesRef("lower"),
            new BytesRef("upper"), false, false);
        assertEquals(expected, ft.rangeQuery("lower", "upper", false, false, null));

        expected = new TermRangeQuery("field",
            new BytesRef("lower"),
            new BytesRef("upper"), true, true);
        assertEquals(expected, ft.rangeQuery("lower", "upper", true, true, null));
    }

    public void testRegexpQuery() {
        RootJsonFieldType ft = createDefaultFieldType();
        ft.setName("field");

        UnsupportedOperationException e  = expectThrows(UnsupportedOperationException.class,
            () -> ft.regexpQuery("valu*", 0, 10, null, null));
        assertEquals("[regexp] queries are not currently supported on [json] fields.", e.getMessage());
    }

    public void testWildcardQuery() {
        RootJsonFieldType ft = createDefaultFieldType();
        ft.setName("field");

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
            () -> ft.wildcardQuery("valu*", null, null));
        assertEquals("[wildcard] queries are not currently supported on [json] fields.", e.getMessage());
    }
}
