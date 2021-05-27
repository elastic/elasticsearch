/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugin.analysis.icu.AnalysisICUPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;

public class ICUCollationKeywordFieldMapperIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AnalysisICUPlugin.class);
    }

    /*
    * Turkish has some funny casing.
    * This test shows how you can solve this kind of thing easily with collation.
    * Instead of using LowerCaseFilter, use a turkish collator with primary strength.
    * Then things will sort and match correctly.
    */
    public void testBasicUsage() throws Exception {
        String index = "foo";

        String[] equivalent = {"I WİLL USE TURKİSH CASING", "ı will use turkish casıng"};

        XContentBuilder builder = jsonBuilder()
            .startObject().startObject("properties")
            .startObject("id")
            .field("type", "keyword")
            .endObject()
            .startObject("collate")
            .field("type", "icu_collation_keyword")
            .field("language", "tr")
            .field("strength", "primary")
            .endObject()
            .endObject().endObject();

        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        // both values should collate to same value
        indexRandom(true,
            client().prepareIndex(index).setId("1")
                .setSource("{\"id\":\"1\",\"collate\":\"" + equivalent[0] + "\"}", XContentType.JSON),
            client().prepareIndex(index).setId("2")
                .setSource("{\"id\":\"2\",\"collate\":\"" + equivalent[1] + "\"}", XContentType.JSON)
        );

        // searching for either of the terms should return both results since they collate to the same value
        SearchRequest request = new SearchRequest()
            .indices(index)
            .source(new SearchSourceBuilder()
                .fetchSource(false)
                .query(QueryBuilders.termQuery("collate", randomBoolean() ? equivalent[0] : equivalent[1]))
                .sort("collate")
                .sort("id", SortOrder.DESC) // secondary sort should kick in because both will collate to same value
            );

        SearchResponse response = client().search(request).actionGet();
        assertNoFailures(response);
        assertHitCount(response, 2L);
        assertOrderedSearchHits(response, "2", "1");
    }

    public void testMultipleValues() throws Exception {
        String index = "foo";

        String[] equivalent = {"a", "C", "a", "B"};

        XContentBuilder builder = jsonBuilder()
            .startObject().startObject("properties")
            .startObject("id")
            .field("type", "keyword")
            .endObject()
            .startObject("collate")
            .field("type", "icu_collation_keyword")
            .field("language", "en")
            .endObject()
            .endObject().endObject();

        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        // everything should be indexed fine, no exceptions
        indexRandom(true,
            client().prepareIndex(index).setId("1")
                .setSource("{\"id\":\"1\", \"collate\":[\"" + equivalent[0] + "\", \"" + equivalent[1] + "\"]}", XContentType.JSON),
            client().prepareIndex(index).setId("2")
                .setSource("{\"id\":\"2\",\"collate\":\"" + equivalent[2] + "\"}", XContentType.JSON)
        );

        // using sort mode = max, values B and C will be used for the sort
        SearchRequest request = new SearchRequest()
            .indices(index)
            .source(new SearchSourceBuilder()
                .fetchSource(false)
                .query(QueryBuilders.termQuery("collate", "a"))
                // if mode max we use c and b as sort values, if max we use "a" for both
                .sort(SortBuilders.fieldSort("collate").sortMode(SortMode.MAX).order(SortOrder.DESC))
                .sort("id", SortOrder.DESC) // will be ignored
            );

        SearchResponse response = client().search(request).actionGet();
        assertNoFailures(response);
        assertHitCount(response, 2L);
        assertOrderedSearchHits(response, "1", "2");

        // same thing, using different sort mode that will use a for both docs
        request = new SearchRequest()
            .indices(index)
            .source(new SearchSourceBuilder()
                .fetchSource(false)
                .query(QueryBuilders.termQuery("collate", "a"))
                // if mode max we use c and b as sort values, if max we use "a" for both
                .sort(SortBuilders.fieldSort("collate").sortMode(SortMode.MIN).order(SortOrder.DESC))
                .sort("id", SortOrder.DESC) // will NOT be ignored and will determine order
            );

        response = client().search(request).actionGet();
        assertNoFailures(response);
        assertHitCount(response, 2L);
        assertOrderedSearchHits(response, "2", "1");
    }

    /*
    * Test usage of the decomposition option for unicode normalization.
    */
    public void testNormalization() throws Exception {
        String index = "foo";

        String[] equivalent = {"I W\u0049\u0307LL USE TURKİSH CASING", "ı will use turkish casıng"};

        XContentBuilder builder = jsonBuilder()
            .startObject().startObject("properties")
            .startObject("id")
            .field("type", "keyword")
            .endObject()
            .startObject("collate")
            .field("type", "icu_collation_keyword")
            .field("language", "tr")
            .field("strength", "primary")
            .field("decomposition", "canonical")
            .endObject()
            .endObject().endObject();

        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        indexRandom(true,
            client().prepareIndex(index).setId("1")
                .setSource("{\"id\":\"1\",\"collate\":\"" + equivalent[0] + "\"}", XContentType.JSON),
            client().prepareIndex(index).setId("2")
                .setSource("{\"id\":\"2\",\"collate\":\"" + equivalent[1] + "\"}", XContentType.JSON)
        );

        // searching for either of the terms should return both results since they collate to the same value
        SearchRequest request = new SearchRequest()
            .indices(index)
            .source(new SearchSourceBuilder()
                .fetchSource(false)
                .query(QueryBuilders.termQuery("collate", randomBoolean() ? equivalent[0] : equivalent[1]))
                .sort("collate")
                .sort("id", SortOrder.DESC) // secondary sort should kick in because both will collate to same value
            );

        SearchResponse response = client().search(request).actionGet();
        assertNoFailures(response);
        assertHitCount(response, 2L);
        assertOrderedSearchHits(response, "2", "1");
    }

    /*
    * Test secondary strength, for english case is not significant.
    */
    public void testSecondaryStrength() throws Exception {
        String index = "foo";

        String[] equivalent = {"TESTING", "testing"};

        XContentBuilder builder = jsonBuilder()
            .startObject().startObject("properties")
            .startObject("id")
            .field("type", "keyword")
            .endObject()
            .startObject("collate")
            .field("type", "icu_collation_keyword")
            .field("language", "en")
            .field("strength", "secondary")
            .field("decomposition", "no")
            .endObject()
            .endObject().endObject();

        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        indexRandom(true,
            client().prepareIndex(index).setId("1")
                .setSource("{\"id\":\"1\",\"collate\":\"" + equivalent[0] + "\"}", XContentType.JSON),
            client().prepareIndex(index).setId("2")
                .setSource("{\"id\":\"2\",\"collate\":\"" + equivalent[1] + "\"}", XContentType.JSON)
        );

        SearchRequest request = new SearchRequest()
            .indices(index)
            .source(new SearchSourceBuilder()
                .fetchSource(false)
                .query(QueryBuilders.termQuery("collate", randomBoolean() ? equivalent[0] : equivalent[1]))
                .sort("collate")
                .sort("id", SortOrder.DESC) // secondary sort should kick in because both will collate to same value
            );

        SearchResponse response = client().search(request).actionGet();
        assertNoFailures(response);
        assertHitCount(response, 2L);
        assertOrderedSearchHits(response, "2", "1");
    }

    /*
    * Setting alternate=shifted to shift whitespace, punctuation and symbols
    * to quaternary level
    */
    public void testIgnorePunctuation() throws Exception {
        String index = "foo";

        String[] equivalent = {"foo-bar", "foo bar"};

        XContentBuilder builder = jsonBuilder()
            .startObject().startObject("properties")
            .startObject("id")
            .field("type", "keyword")
            .endObject()
            .startObject("collate")
            .field("type", "icu_collation_keyword")
            .field("language", "en")
            .field("strength", "primary")
            .field("alternate", "shifted")
            .endObject()
            .endObject().endObject();

        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        indexRandom(true,
            client().prepareIndex(index).setId("1").setSource("{\"id\":\"1\",\"collate\":\"" + equivalent[0] + "\"}", XContentType.JSON),
            client().prepareIndex(index).setId("2").setSource("{\"id\":\"2\",\"collate\":\"" + equivalent[1] + "\"}", XContentType.JSON)
        );

        SearchRequest request = new SearchRequest()
            .indices(index)
            .source(new SearchSourceBuilder()
                .fetchSource(false)
                .query(QueryBuilders.termQuery("collate", randomBoolean() ? equivalent[0] : equivalent[1]))
                .sort("collate")
                .sort("id", SortOrder.DESC) // secondary sort should kick in because both will collate to same value
            );

        SearchResponse response = client().search(request).actionGet();
        assertNoFailures(response);
        assertHitCount(response, 2L);
        assertOrderedSearchHits(response, "2", "1");
    }

    /*
    * Setting alternate=shifted and variableTop to shift whitespace, but not
    * punctuation or symbols, to quaternary level
    */
    public void testIgnoreWhitespace() throws Exception {
        String index = "foo";

        XContentBuilder builder = jsonBuilder()
            .startObject().startObject("properties")
            .startObject("id")
            .field("type", "keyword")
            .endObject()
            .startObject("collate")
            .field("type", "icu_collation_keyword")
            .field("language", "en")
            .field("strength", "primary")
            .field("alternate", "shifted")
            .field("variable_top", " ")
            .field("index", false)
            .endObject()
            .endObject().endObject();

        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        indexRandom(true,
            client().prepareIndex(index).setId("1").setSource("{\"id\":\"1\",\"collate\":\"foo bar\"}", XContentType.JSON),
            client().prepareIndex(index).setId("2").setSource("{\"id\":\"2\",\"collate\":\"foobar\"}", XContentType.JSON),
            client().prepareIndex(index).setId("3").setSource("{\"id\":\"3\",\"collate\":\"foo-bar\"}", XContentType.JSON)
        );

        SearchRequest request = new SearchRequest()
            .indices(index)
            .source(new SearchSourceBuilder()
                .fetchSource(false)
                .sort("collate", SortOrder.ASC)
                .sort("id", SortOrder.ASC) // secondary sort should kick in on docs 1 and 3 because same value collate value
            );

        SearchResponse response = client().search(request).actionGet();
        assertNoFailures(response);
        assertHitCount(response, 3L);
        assertOrderedSearchHits(response, "3", "1", "2");
    }

    /*
     * Setting numeric to encode digits with numeric value, so that
     * foobar-9 sorts before foobar-10
     */
    public void testNumerics() throws Exception {
        String index = "foo";

        XContentBuilder builder = jsonBuilder()
            .startObject().startObject("properties")
            .startObject("collate")
            .field("type", "icu_collation_keyword")
            .field("language", "en")
            .field("numeric", true)
            .field("index", false)
            .endObject()
            .endObject().endObject();

        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        indexRandom(true,
            client().prepareIndex(index).setId("1").setSource("{\"collate\":\"foobar-10\"}", XContentType.JSON),
            client().prepareIndex(index).setId("2").setSource("{\"collate\":\"foobar-9\"}", XContentType.JSON)
        );

        SearchRequest request = new SearchRequest()
            .indices(index)
            .source(new SearchSourceBuilder()
                .fetchSource(false)
                .sort("collate", SortOrder.ASC)
            );

        SearchResponse response = client().search(request).actionGet();
        assertNoFailures(response);
        assertHitCount(response, 2L);
        assertOrderedSearchHits(response, "2", "1");
    }

    /*
    * Setting caseLevel=true to create an additional case level between
    * secondary and tertiary
    */
    public void testIgnoreAccentsButNotCase() throws Exception {
        String index = "foo";

        XContentBuilder builder = jsonBuilder()
            .startObject().startObject("properties")
            .startObject("id")
            .field("type", "keyword")
            .endObject()
            .startObject("collate")
            .field("type", "icu_collation_keyword")
            .field("language", "en")
            .field("strength", "primary")
            .field("case_level", true)
            .field("index", false)
            .endObject()
            .endObject().endObject();

        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        indexRandom(true,
            client().prepareIndex(index).setId("1").setSource("{\"id\":\"1\",\"collate\":\"résumé\"}", XContentType.JSON),
            client().prepareIndex(index).setId("2").setSource("{\"id\":\"2\",\"collate\":\"Resume\"}", XContentType.JSON),
            client().prepareIndex(index).setId("3").setSource("{\"id\":\"3\",\"collate\":\"resume\"}", XContentType.JSON),
            client().prepareIndex(index).setId("4").setSource("{\"id\":\"4\",\"collate\":\"Résumé\"}", XContentType.JSON)
        );

        SearchRequest request = new SearchRequest()
            .indices(index)
            .source(new SearchSourceBuilder()
                .fetchSource(false)
                .sort("collate", SortOrder.ASC)
                .sort("id", SortOrder.DESC)
            );

        SearchResponse response = client().search(request).actionGet();
        assertNoFailures(response);
        assertHitCount(response, 4L);
        assertOrderedSearchHits(response, "3", "1", "4", "2");
    }

    /*
    * Setting caseFirst=upper to cause uppercase strings to sort
    * before lowercase ones.
    */
    public void testUpperCaseFirst() throws Exception {
        String index = "foo";

        XContentBuilder builder = jsonBuilder()
            .startObject().startObject("properties")
            .startObject("collate")
            .field("type", "icu_collation_keyword")
            .field("language", "en")
            .field("strength", "tertiary")
            .field("case_first", "upper")
            .field("index", false)
            .endObject()
            .endObject().endObject();

        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        indexRandom(true,
            client().prepareIndex(index).setId("1").setSource("{\"collate\":\"resume\"}", XContentType.JSON),
            client().prepareIndex(index).setId("2").setSource("{\"collate\":\"Resume\"}", XContentType.JSON)
        );

        SearchRequest request = new SearchRequest()
            .indices(index)
            .source(new SearchSourceBuilder()
                .fetchSource(false)
                .sort("collate", SortOrder.ASC)
            );

        SearchResponse response = client().search(request).actionGet();
        assertNoFailures(response);
        assertHitCount(response, 2L);
        assertOrderedSearchHits(response, "2", "1");
    }

    /*
    * For german, you might want oe to sort and match with o umlaut.
    * This is not the default, but you can make a customized ruleset to do this.
    *
    * The default is DIN 5007-1, this shows how to tailor a collator to get DIN 5007-2 behavior.
    *  http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4423383
    */
    public void testCustomRules() throws Exception {
        String index = "foo";

        RuleBasedCollator baseCollator = (RuleBasedCollator) Collator.getInstance(new ULocale("de_DE"));
        String DIN5007_2_tailorings =
            "& ae , a\u0308 & AE , A\u0308" +
                "& oe , o\u0308 & OE , O\u0308" +
                "& ue , u\u0308 & UE , u\u0308";

        RuleBasedCollator tailoredCollator = new RuleBasedCollator(baseCollator.getRules() + DIN5007_2_tailorings);
        String tailoredRules = tailoredCollator.getRules();

        String[] equivalent = {"Töne", "Toene"};

        XContentBuilder builder = jsonBuilder()
            .startObject().startObject("properties")
            .startObject("id")
            .field("type", "keyword")
            .endObject()
            .startObject("collate")
            .field("type", "icu_collation_keyword")
            .field("rules", tailoredRules)
            .field("strength", "primary")
            .endObject()
            .endObject().endObject();

        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        indexRandom(true,
            client().prepareIndex(index).setId("1").setSource("{\"id\":\"1\",\"collate\":\"" + equivalent[0] + "\"}", XContentType.JSON),
            client().prepareIndex(index).setId("2").setSource("{\"id\":\"2\",\"collate\":\"" + equivalent[1] + "\"}", XContentType.JSON)
        );

        SearchRequest request = new SearchRequest()
            .indices(index)
            .source(new SearchSourceBuilder()
                .fetchSource(false)
                .query(QueryBuilders.termQuery("collate", randomBoolean() ? equivalent[0] : equivalent[1]))
                .sort("collate", SortOrder.ASC)
                .sort("id", SortOrder.DESC) // secondary sort should kick in because both will collate to same value
            );

        SearchResponse response = client().search(request).actionGet();
        assertNoFailures(response);
        assertHitCount(response, 2L);
        assertOrderedSearchHits(response, "2", "1");
    }
}
