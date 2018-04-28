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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.Fuzziness;
import org.junit.Before;

public class TextFieldTypeTests extends FieldTypeTestCase {

    protected static final String TEXTFIELD = "textField1";

    @Before
    public void setupProperties() {
        addModifier(new Modifier("fielddata", true) {
            @Override
            public void modify(MappedFieldType ft) {
                TextFieldMapper.TextFieldType tft = toTextFieldType(ft);
                tft.setFielddata(tft.fielddata() == false);
            }
        });
        addModifier(new Modifier("fielddata_frequency_filter.min", true) {
            @Override
            public void modify(MappedFieldType ft) {
                TextFieldMapper.TextFieldType tft = toTextFieldType(ft);
                tft.setFielddataMinFrequency(3);
            }
        });
        addModifier(new Modifier("fielddata_frequency_filter.max", true) {
            @Override
            public void modify(MappedFieldType ft) {
                TextFieldMapper.TextFieldType tft = toTextFieldType(ft);
                tft.setFielddataMaxFrequency(0.2);
            }
        });
        addModifier(new Modifier("fielddata_frequency_filter.min_segment_size", true) {
            @Override
            public void modify(MappedFieldType ft) {
                TextFieldMapper.TextFieldType tft = toTextFieldType(ft);
                tft.setFielddataMinSegmentSize(1000);
            }
        });
    }

    public void testTermQuery() {
        MappedFieldType ft = this.createNamedDefaultFieldType();
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(new TermQuery(new Term(this.fieldInQuery(), "foo")), ft.termQuery("foo", null));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.termQuery("bar", null));
        assertEquals("Cannot search on field "+ this.fieldInMessage() + " since it is not indexed.", e.getMessage());
    }

    public void testTermsQuery() {
        MappedFieldType ft = this.createNamedDefaultFieldType();
        ft.setIndexOptions(IndexOptions.DOCS);
        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef("foo"));
        terms.add(new BytesRef("bar"));
        assertEquals(new TermInSetQuery(this.fieldInQuery(), terms),
                ft.termsQuery(Arrays.asList("foo", "bar"), null));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.termsQuery(Arrays.asList("foo", "bar"), null));
        assertEquals("Cannot search on field " + this.fieldInMessage() + " since it is not indexed.", e.getMessage());
    }

    public void testRegexpQuery() {
        MappedFieldType ft = this.createNamedDefaultFieldType();
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(new RegexpQuery(new Term(this.fieldInQuery(),"foo.*")),
                ft.regexpQuery("foo.*", 0, 10, null, null));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.regexpQuery("foo.*", 0, 10, null, null));
        assertEquals("Cannot search on field " + this.fieldInMessage() + " since it is not indexed.", e.getMessage());
    }

    public void testFuzzyQuery() {
        MappedFieldType ft = this.createNamedDefaultFieldType();
        ft.setIndexOptions(IndexOptions.DOCS);
        assertEquals(new FuzzyQuery(new Term(this.fieldInQuery(),"foo"), 2, 1, 50, true),
                ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true));
        assertEquals("Cannot search on field " + this.fieldInMessage() + " since it is not indexed.", e.getMessage());
    }

    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new TextFieldMapper.TextFieldType();
    }

    @Override
    protected String fieldTypeName() {
        return TEXTFIELD;
    }

    String fieldInQuery() {
        return TEXTFIELD;
    }

    String fieldInMessage() {
        return MappedFieldType.nameInMessage(TEXTFIELD);
    }

    TextFieldMapper.TextFieldType toTextFieldType(final MappedFieldType fieldType) {
        return (TextFieldMapper.TextFieldType) fieldType;
    }
}
