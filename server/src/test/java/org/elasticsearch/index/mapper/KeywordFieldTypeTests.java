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

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class KeywordFieldTypeTests extends FieldTypeTestCase {

    public void testIsFieldWithinQuery() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        // current impl ignores args and should always return INTERSECTS
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(null,
                RandomStrings.randomAsciiOfLengthBetween(random(), 0, 5),
                RandomStrings.randomAsciiOfLengthBetween(random(), 0, 5),
                randomBoolean(), randomBoolean(), null, null, null));
    }

    public void testTermQuery() {
        MappedFieldType ft = new KeywordFieldType("field");
        assertEquals(new TermQuery(new Term("field", "foo")), ft.termQuery("foo", null));

        MappedFieldType unsearchable = new KeywordFieldType("field", false, true, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> unsearchable.termQuery("bar", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testTermQueryWithNormalizer() {
        Analyzer normalizer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer in = new WhitespaceTokenizer();
                TokenFilter out = new LowerCaseFilter(in);
                return new TokenStreamComponents(in, out);
            }
            @Override
            protected TokenStream normalize(String fieldName, TokenStream in) {
                return new LowerCaseFilter(in);
            }
        };
        MappedFieldType ft = new KeywordFieldType("field", new NamedAnalyzer("my_normalizer", AnalyzerScope.INDEX, normalizer));
        assertEquals(new TermQuery(new Term("field", "foo bar")), ft.termQuery("fOo BaR", null));
    }

    public void testTermsQuery() {
        MappedFieldType ft = new KeywordFieldType("field");
        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef("foo"));
        terms.add(new BytesRef("bar"));
        assertEquals(new TermInSetQuery("field", terms),
                ft.termsQuery(Arrays.asList("foo", "bar"), null));

        MappedFieldType unsearchable = new KeywordFieldType("field", false, true, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> unsearchable.termsQuery(Arrays.asList("foo", "bar"), null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testExistsQuery() {
        KeywordFieldType ft = new KeywordFieldType("field");
        ft.hasNorms = false;
        assertEquals(new DocValuesFieldExistsQuery("field"), ft.existsQuery(null));

        ft = new KeywordFieldType("field", true, false, Collections.emptyMap());
        ft.hasNorms = true;
        assertEquals(new NormsFieldExistsQuery("field"), ft.existsQuery(null));

        ft = new KeywordFieldType("field", true, false, Collections.emptyMap());
        ft.hasNorms = false;
        assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.NAME, "field")), ft.existsQuery(null));
    }

    public void testRangeQuery() {
        MappedFieldType ft = new KeywordFieldType("field");
        assertEquals(new TermRangeQuery("field", BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("bar"), true, false),
                ft.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_QSC));

        ElasticsearchException ee = expectThrows(ElasticsearchException.class,
                () -> ft.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_QSC_DISALLOW_EXPENSIVE));
        assertEquals("[range] queries on [text] or [keyword] fields cannot be executed when " +
                "'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testRegexpQuery() {
        MappedFieldType ft = new KeywordFieldType("field");
        assertEquals(new RegexpQuery(new Term("field","foo.*")),
                ft.regexpQuery("foo.*", 0, 10, null, MOCK_QSC));

        MappedFieldType unsearchable = new KeywordFieldType("field", false, true, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> unsearchable.regexpQuery("foo.*", 0, 10, null, MOCK_QSC));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());

        ElasticsearchException ee = expectThrows(ElasticsearchException.class,
                () -> ft.regexpQuery("foo.*", randomInt(10), randomInt(10) + 1, null, MOCK_QSC_DISALLOW_EXPENSIVE));
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.",
                ee.getMessage());
    }

    public void testFuzzyQuery() {
        MappedFieldType ft = new KeywordFieldType("field");
        assertEquals(new FuzzyQuery(new Term("field","foo"), 2, 1, 50, true),
                ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MOCK_QSC));

        MappedFieldType unsearchable = new KeywordFieldType("field", false, true, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> unsearchable.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MOCK_QSC));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());

        ElasticsearchException ee = expectThrows(ElasticsearchException.class,
                () -> ft.fuzzyQuery("foo", Fuzziness.AUTO, randomInt(10) + 1, randomInt(10) + 1,
                        randomBoolean(), MOCK_QSC_DISALLOW_EXPENSIVE));
        assertEquals("[fuzzy] queries cannot be executed when 'search.allow_expensive_queries' is set to false.",
                ee.getMessage());
    }

    public void testNormalizeQueries() {
        MappedFieldType ft = new KeywordFieldType("field");
        assertEquals(new TermQuery(new Term("field", new BytesRef("FOO"))), ft.termQuery("FOO", null));
        ft = new KeywordFieldType("field", Lucene.STANDARD_ANALYZER);
        assertEquals(new TermQuery(new Term("field", new BytesRef("foo"))), ft.termQuery("FOO", null));
    }
}
