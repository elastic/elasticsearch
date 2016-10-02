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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class SimpleQueryParserTests extends ESTestCase {
    private static class MockSimpleQueryParser extends SimpleQueryParser {
        public MockSimpleQueryParser(Analyzer analyzer, Map<String, Float> weights, int flags, Settings settings) {
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

}
