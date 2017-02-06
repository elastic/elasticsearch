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

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockSynonymAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.GraphQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MockFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class SimpleQueryParserTests extends ESTestCase {
    private static class MockSimpleQueryParser extends SimpleQueryParser {
        MockSimpleQueryParser(Analyzer analyzer, Map<String, Float> weights, int flags, Settings settings) {
            super(analyzer, weights, flags, settings, null);
        }

        @Override
        protected Query newTermQuery(Term term) {
            return new TermQuery(term);
        }
    }

    public void testAnalyzeWildcard() {
        SimpleQueryParser.Settings settings = new SimpleQueryParser.Settings();
        settings.analyzeWildcard(true);
        Map<String, Float> weights = new HashMap<>();
        weights.put("field1", 1.0f);
        SimpleQueryParser parser = new MockSimpleQueryParser(new StandardAnalyzer(), weights, -1, settings);
        for (Operator op : Operator.values()) {
            BooleanClause.Occur defaultOp = op.toBooleanClauseOccur();
            parser.setDefaultOperator(defaultOp);
            Query query = parser.parse("first foo-bar-foobar* last");
            Query expectedQuery =
                new BooleanQuery.Builder()
                    .add(new BooleanClause(new TermQuery(new Term("field1", "first")), defaultOp))
                    .add(new BooleanQuery.Builder()
                        .add(new BooleanClause(new TermQuery(new Term("field1", "foo")), defaultOp))
                        .add(new BooleanClause(new TermQuery(new Term("field1", "bar")), defaultOp))
                        .add(new BooleanClause(new PrefixQuery(new Term("field1", "foobar")), defaultOp))
                        .build(), defaultOp)
                    .add(new BooleanClause(new TermQuery(new Term("field1", "last")), defaultOp))
                    .build();
            assertThat(query, equalTo(expectedQuery));
        }
    }

    public void testAnalyzerWildcardWithSynonyms() {
        SimpleQueryParser.Settings settings = new SimpleQueryParser.Settings();
        settings.analyzeWildcard(true);
        Map<String, Float> weights = new HashMap<>();
        weights.put("field1", 1.0f);
        SimpleQueryParser parser = new MockSimpleQueryParser(new MockRepeatAnalyzer(), weights, -1, settings);

        for (Operator op : Operator.values()) {
            BooleanClause.Occur defaultOp = op.toBooleanClauseOccur();
            parser.setDefaultOperator(defaultOp);
            Query query = parser.parse("first foo-bar-foobar* last");

            Query expectedQuery = new BooleanQuery.Builder()
                .add(new BooleanClause(new SynonymQuery(new Term("field1", "first"),
                    new Term("field1", "first")), defaultOp))
                .add(new BooleanQuery.Builder()
                    .add(new BooleanClause(new SynonymQuery(new Term("field1", "foo"),
                        new Term("field1", "foo")), defaultOp))
                    .add(new BooleanClause(new SynonymQuery(new Term("field1", "bar"),
                        new Term("field1", "bar")), defaultOp))
                    .add(new BooleanQuery.Builder()
                        .add(new BooleanClause(new PrefixQuery(new Term("field1", "foobar")),
                            BooleanClause.Occur.SHOULD))
                        .add(new BooleanClause(new PrefixQuery(new Term("field1", "foobar")),
                            BooleanClause.Occur.SHOULD))
                        .setDisableCoord(true)
                        .build(), defaultOp)
                    .build(), defaultOp)
                .add(new BooleanClause(new SynonymQuery(new Term("field1", "last"),
                    new Term("field1", "last")), defaultOp))
                .build();
            assertThat(query, equalTo(expectedQuery));
        }
    }

    public void testAnalyzerWithGraph() {
        SimpleQueryParser.Settings settings = new SimpleQueryParser.Settings();
        settings.analyzeWildcard(true);
        Map<String, Float> weights = new HashMap<>();
        weights.put("field1", 1.0f);
        SimpleQueryParser parser = new MockSimpleQueryParser(new MockSynonymAnalyzer(), weights, -1, settings);

        for (Operator op : Operator.values()) {
            BooleanClause.Occur defaultOp = op.toBooleanClauseOccur();
            parser.setDefaultOperator(defaultOp);

            // non-phrase won't detect multi-word synonym because of whitespace splitting
            Query query = parser.parse("guinea pig");

            Query expectedQuery = new BooleanQuery.Builder()
                .add(new BooleanClause(new TermQuery(new Term("field1", "guinea")), defaultOp))
                .add(new BooleanClause(new TermQuery(new Term("field1", "pig")), defaultOp))
                .build();
            assertThat(query, equalTo(expectedQuery));

            // phrase will pick it up
            query = parser.parse("\"guinea pig\"");

            expectedQuery = new GraphQuery(
                new PhraseQuery("field1", "guinea", "pig"),
                new TermQuery(new Term("field1", "cavy")));

            assertThat(query, equalTo(expectedQuery));

            // phrase with slop
            query = parser.parse("big \"guinea pig\"~2");

            expectedQuery = new BooleanQuery.Builder()
                .add(new BooleanClause(new TermQuery(new Term("field1", "big")), defaultOp))
                .add(new BooleanClause(new GraphQuery(
                    new PhraseQuery.Builder()
                        .add(new Term("field1", "guinea"))
                        .add(new Term("field1", "pig"))
                        .setSlop(2)
                        .build(),
                    new TermQuery(new Term("field1", "cavy"))), defaultOp))
                .build();

            assertThat(query, equalTo(expectedQuery));
        }
    }

    public void testQuoteFieldSuffix() {
        SimpleQueryParser.Settings sqpSettings = new SimpleQueryParser.Settings();
        sqpSettings.quoteFieldSuffix(".quote");

        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_INDEX_UUID, "some_uuid")
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
        IndexMetaData indexState = IndexMetaData.builder("index").settings(indexSettings).build();
        IndexSettings settings = new IndexSettings(indexState, Settings.EMPTY);
        QueryShardContext mockShardContext = new QueryShardContext(0, settings, null, null, null, null, null, xContentRegistry(),
                null, null, System::currentTimeMillis) {
            @Override
            public MappedFieldType fieldMapper(String name) {
                return new MockFieldMapper.FakeFieldType();
            }
        };

        SimpleQueryParser parser = new SimpleQueryParser(new StandardAnalyzer(),
                Collections.singletonMap("foo", 1f), -1, sqpSettings, mockShardContext);
        assertEquals(new TermQuery(new Term("foo", "bar")), parser.parse("bar"));
        assertEquals(new TermQuery(new Term("foo.quote", "bar")), parser.parse("\"bar\""));

        // Now check what happens if foo.quote does not exist
        mockShardContext = new QueryShardContext(0, settings, null, null, null, null, null, xContentRegistry(),
                null, null, System::currentTimeMillis) {
            @Override
            public MappedFieldType fieldMapper(String name) {
                if (name.equals("foo.quote")) {
                    return null;
                }
                return new MockFieldMapper.FakeFieldType();
            }
        };
        parser = new SimpleQueryParser(new StandardAnalyzer(),
                Collections.singletonMap("foo", 1f), -1, sqpSettings, mockShardContext);
        assertEquals(new TermQuery(new Term("foo", "bar")), parser.parse("bar"));
        assertEquals(new TermQuery(new Term("foo", "bar")), parser.parse("\"bar\""));
    }
}
