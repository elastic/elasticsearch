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
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.unit.Fuzziness;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.lucene.search.MultiTermQuery.CONSTANT_SCORE_REWRITE;
import static org.hamcrest.Matchers.equalTo;

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
        addModifier(new Modifier("index_phrases", false) {
            @Override
            public void modify(MappedFieldType ft) {
                TextFieldMapper.TextFieldType tft = (TextFieldMapper.TextFieldType) ft;
                tft.setIndexPhrases(true);
            }
        });
        addModifier(new Modifier("index_prefixes", false) {
            @Override
            public void modify(MappedFieldType ft) {
                TextFieldMapper.TextFieldType tft = (TextFieldMapper.TextFieldType)ft;
                TextFieldMapper.PrefixFieldType pft = tft.getPrefixFieldType();
                if (pft == null) {
                    tft.setPrefixFieldType(new TextFieldMapper.PrefixFieldType(ft.name(), ft.name() + "._index_prefix", 3, 3));
                }
                else {
                    tft.setPrefixFieldType(null);
                }
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
        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef("foo"));
        terms.add(new BytesRef("bar"));
        assertEquals(new TermInSetQuery("field", terms),
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

    public void testIndexPrefixes() {
        TextFieldMapper.TextFieldType ft = new TextFieldMapper.TextFieldType();
        ft.setName("field");
        ft.setPrefixFieldType(new TextFieldMapper.PrefixFieldType("field", "field._index_prefix", 2, 10));

        Query q = ft.prefixQuery("goin", CONSTANT_SCORE_REWRITE, null);
        assertEquals(new ConstantScoreQuery(new TermQuery(new Term("field._index_prefix", "goin"))), q);

        q = ft.prefixQuery("internationalisatio", CONSTANT_SCORE_REWRITE, null);
        assertEquals(new PrefixQuery(new Term("field", "internationalisatio")), q);

        q = ft.prefixQuery("g", CONSTANT_SCORE_REWRITE, null);
        Automaton automaton
            = Operations.concatenate(Arrays.asList(Automata.makeChar('g'), Automata.makeAnyChar()));

        Query expected = new ConstantScoreQuery(new BooleanQuery.Builder()
            .add(new AutomatonQuery(new Term("field._index_prefix", "g*"), automaton), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("field", "g")), BooleanClause.Occur.SHOULD)
            .build());

        assertThat(q, equalTo(expected));
    }
}
