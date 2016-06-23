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

package org.apache.lucene.queryparser.classic;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.ESSingleNodeTestCase;

public class MapperQueryParserTests extends ESSingleNodeTestCase {

    public void testTermQuery() throws ParseException {
        IndexService index = createIndex("index", Settings.EMPTY, "type", "field", "type=text,analyzer=english");
        QueryShardContext context = index.newQueryShardContext();
        MapperQueryParser parser = new MapperQueryParser(context);
        QueryParserSettings settings = new QueryParserSettings();
        settings.defaultField("field");
        parser.reset(settings);
        Query query = parser.parse("Foxes");
        assertEquals(new TermQuery(new Term("field", "fox")), query); // the whole chain was applied
    }

    public void testPhraseQuery() throws ParseException {
        IndexService index = createIndex("index", Settings.EMPTY, "type", "field", "type=text,analyzer=english");
        QueryShardContext context = index.newQueryShardContext();
        MapperQueryParser parser = new MapperQueryParser(context);
        QueryParserSettings settings = new QueryParserSettings();
        settings.defaultField("field");
        parser.reset(settings);
        Query query = parser.parse("\"Quick Foxes\"");
        assertEquals(new BooleanQuery.Builder()
                .setDisableCoord(true)
                .add(new PhraseQuery("field", "quick", "fox"), Occur.SHOULD) // the whole chain was applied
                .build(),
                query);
    }

    public void testPrefixQuery() throws ParseException {
        IndexService index = createIndex("index", Settings.EMPTY, "type", "field", "type=text,analyzer=english");
        QueryShardContext context = index.newQueryShardContext();
        MapperQueryParser parser = new MapperQueryParser(context);
        QueryParserSettings settings = new QueryParserSettings();
        settings.defaultField("field");
        parser.reset(settings);
        Query query = parser.parse("Tables*");
        assertEquals(new PrefixQuery(new Term("field", "tables")), query); // lowercase was applied but not stemming

        settings.analyzeWildcard(true);
        parser.reset(settings);
        query = parser.parse("Tables*");
        assertEquals(new PrefixQuery(new Term("field", "tabl")), query);
    }

    public void testWildcardQuery() throws ParseException {
        IndexService index = createIndex("index", Settings.EMPTY, "type", "field", "type=text,analyzer=english");
        QueryShardContext context = index.newQueryShardContext();
        MapperQueryParser parser = new MapperQueryParser(context);
        QueryParserSettings settings = new QueryParserSettings();
        settings.defaultField("field");
        parser.reset(settings);
        Query query = parser.parse("Fr*days");
        assertEquals(new WildcardQuery(new Term("field", "fr*days")), query); // lowercase was applied but not stemming

        settings.analyzeWildcard(true);
        parser.reset(settings);
        query = parser.parse("Fr*days");
        assertEquals(new WildcardQuery(new Term("field", "fr*dai")), query);
    }

    public void testFuzzyQuery() throws ParseException {
        IndexService index = createIndex("index", Settings.EMPTY, "type", "field", "type=text,analyzer=english");
        QueryShardContext context = index.newQueryShardContext();
        MapperQueryParser parser = new MapperQueryParser(context);
        QueryParserSettings settings = new QueryParserSettings();
        settings.defaultField("field");
        parser.reset(settings);
        Query query = parser.parse("Toys~1");
        assertEquals(new FuzzyQuery(new Term("field", "toys"), 1), query); // lowercase was applied but not stemming
    }

    public void testRangeQuery() throws ParseException {
        IndexService index = createIndex("index", Settings.EMPTY, "type", "field", "type=text,analyzer=english");
        QueryShardContext context = index.newQueryShardContext();
        MapperQueryParser parser = new MapperQueryParser(context);
        QueryParserSettings settings = new QueryParserSettings();
        settings.defaultField("field");
        parser.reset(settings);
        Query query = parser.parse("[A TO B]");
        assertEquals(new TermRangeQuery("field", new BytesRef("a"), new BytesRef("b"),
                true, true), query); // lowercase was applied but not stemming
    }

}
