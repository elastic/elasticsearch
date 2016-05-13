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

import java.util.Arrays;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.junit.Before;

public class TextFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new TextFieldMapper.TextFieldType();
    }

    @Before
    public void setupProperties() {
        addModifier(new Modifier("fielddata", true) {
            @Override
            public void modify(MappedFieldType ft) {
                TextFieldMapper.TextFieldType tft = (TextFieldMapper.TextFieldType)ft;
                tft.setFielddata(tft.fielddata() == false);
            }
        });
        addModifier(new Modifier("fielddata_frequency_filter.min", true) {
            @Override
            public void modify(MappedFieldType ft) {
                TextFieldMapper.TextFieldType tft = (TextFieldMapper.TextFieldType)ft;
                tft.setFielddataMinFrequency(3);
            }
        });
        addModifier(new Modifier("fielddata_frequency_filter.max", true) {
            @Override
            public void modify(MappedFieldType ft) {
                TextFieldMapper.TextFieldType tft = (TextFieldMapper.TextFieldType)ft;
                tft.setFielddataMaxFrequency(0.2);
            }
        });
        addModifier(new Modifier("fielddata_frequency_filter.min_segment_size", true) {
            @Override
            public void modify(MappedFieldType ft) {
                TextFieldMapper.TextFieldType tft = (TextFieldMapper.TextFieldType)ft;
                tft.setFielddataMinSegmentSize(1000);
            }
        });
    }

    public void testTermQuery() {
        MappedFieldType ft = createDefaultFieldType();
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(new TermQuery(new Term("field", "foo")), ft.termQuery("foo", null));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.termQuery("bar", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testTermsQuery() {
        MappedFieldType ft = createDefaultFieldType();
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(new TermsQuery(new Term("field", "foo"), new Term("field", "bar")),
                ft.termsQuery(Arrays.asList("foo", "bar"), null));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.termsQuery(Arrays.asList("foo", "bar"), null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testRegexpQuery() {
        MappedFieldType ft = createDefaultFieldType();
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(new RegexpQuery(new Term("field","foo.*")),
                ft.regexpQuery("foo.*", 0, 10, null, null));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.regexpQuery("foo.*", 0, 10, null, null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testFuzzyQuery() {
        MappedFieldType ft = createDefaultFieldType();
        ft.setName("field");
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(new FuzzyQuery(new Term("field","foo"), 2, 1, 50, true),
                ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }
}
