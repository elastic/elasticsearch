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

package org.elasticsearch.search.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.spatial.geopoint.search.GeoPointDistanceQuery;
import org.apache.lucene.spatial.geopoint.search.GeoPointInBBoxQuery;
import org.apache.lucene.spatial.geopoint.search.GeoPointInPolygonQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class PlainHighlighterTests extends LuceneTestCase {

    public void testHighlightPhrase() throws Exception {
        Query query = new PhraseQuery.Builder()
                .add(new Term("field", "foo"))
                .add(new Term("field", "bar"))
                .build();
        QueryScorer queryScorer = new CustomQueryScorer(query);
        org.apache.lucene.search.highlight.Highlighter highlighter = new org.apache.lucene.search.highlight.Highlighter(queryScorer);
        String[] frags = highlighter.getBestFragments(new MockAnalyzer(random()), "field", "bar foo bar foo", 10);
        assertArrayEquals(new String[] {"bar <B>foo</B> <B>bar</B> foo"}, frags);
    }

    public void checkGeoQueryHighlighting(Query geoQuery) throws IOException, InvalidTokenOffsetsException {
        Map analysers = new HashMap<String, Analyzer>();
        analysers.put("text", new StandardAnalyzer());
        FieldNameAnalyzer fieldNameAnalyzer = new FieldNameAnalyzer(analysers);
        Query termQuery = new TermQuery(new Term("text", "failure"));
        Query boolQuery = new BooleanQuery.Builder().add(new BooleanClause(geoQuery, BooleanClause.Occur.SHOULD))
            .add(new BooleanClause(termQuery, BooleanClause.Occur.SHOULD)).build();
        org.apache.lucene.search.highlight.Highlighter highlighter =
            new org.apache.lucene.search.highlight.Highlighter(new CustomQueryScorer(boolQuery));
        String fragment = highlighter.getBestFragment(fieldNameAnalyzer.tokenStream("text", "Arbitrary text field which should not cause " +
            "a failure"), "Arbitrary text field which should not cause a failure");
        assertThat(fragment, equalTo("Arbitrary text field which should not cause a <B>failure</B>"));
        // TODO: This test will fail if we pass in an instance of GeoPointInBBoxQueryImpl too. Should we also find a way to work around that
        // or can the query not be rewritten before it is passed into the highlighter?
    }

    public void testGeoPointInBBoxQueryHighlighting() throws IOException, InvalidTokenOffsetsException {
        Query geoQuery = new GeoPointDistanceQuery("geo_point", -64.92354174306496, -170.15625, 5576757);
        checkGeoQueryHighlighting(geoQuery);
    }

    public void testGeoPointDistanceQueryHighlighting() throws IOException, InvalidTokenOffsetsException {
        Query geoQuery = new GeoPointInBBoxQuery("geo_point", -64.92354174306496, 61.10078883158897, -170.15625, 118.47656249999999);
        checkGeoQueryHighlighting(geoQuery);
    }

    public void testGeoPointInPolygonQueryHighlighting() throws IOException, InvalidTokenOffsetsException {
        double[] polyLats = new double[]{0, 60, 0, 0};
        double[] polyLons = new double[]{0, 60, 90, 0};
        Query geoQuery = new GeoPointInPolygonQuery("geo_point", polyLats, polyLons);
        checkGeoQueryHighlighting(geoQuery);
    }
}
