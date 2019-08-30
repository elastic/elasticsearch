/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import static org.elasticsearch.action.search.SearchType.DFS_QUERY_THEN_FETCH;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSecondHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThirdHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;


public class PinnedQueryBuilderIT extends ESIntegTestCase {

    public void testIdInsertionOrderRetained() {
        String[] ids = generateRandomStringArray(10, 50, false);
        PinnedQueryBuilder pqb = new PinnedQueryBuilder(new MatchAllQueryBuilder(), ids);
        List<String> addedIds = pqb.ids();
        int pos = 0;
        for (String key : addedIds) {
            assertEquals(ids[pos++], key);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(SearchBusinessRules.class);
        return plugins;
    }

    public void testPinnedPromotions() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1",
                        jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1")
                                .field("analyzer", "whitespace").field("type", "text").endObject().endObject().endObject().endObject())
                .setSettings(Settings.builder().put(indexSettings()).put("index.number_of_shards", randomIntBetween(2, 5))));

        int numRelevantDocs = randomIntBetween(1, 100);
        for (int i = 0; i < numRelevantDocs; i++) {
            if (i % 2 == 0) {
                // add lower-scoring text
                client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field1", "the quick brown fox").get();
            } else {
                // add higher-scoring text
                client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field1", "red fox").get();
            }
        }
        // Add docs with no relevance
        int numIrrelevantDocs = randomIntBetween(1, 10);
        for (int i = numRelevantDocs; i <= numRelevantDocs + numIrrelevantDocs; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field1", "irrelevant").get();
        }
        refresh();

        // Test doc pinning
        int totalDocs = numRelevantDocs + numIrrelevantDocs;
        for (int i = 0; i < 100; i++) {
            int numPromotions = randomIntBetween(0, totalDocs);

            LinkedHashSet<String> pins = new LinkedHashSet<>();
            for (int j = 0; j < numPromotions; j++) {
                pins.add(Integer.toString(randomIntBetween(0, totalDocs)));
            }
            QueryBuilder organicQuery = null;
            if (i % 5 == 0) {
                // Occasionally try a query with no matches to check all pins still show
                organicQuery = QueryBuilders.matchQuery("field1", "matchNoDocs");
            } else {
                organicQuery = QueryBuilders.matchQuery("field1", "red fox");
            }
            PinnedQueryBuilder pqb = new PinnedQueryBuilder(organicQuery, pins.toArray(new String[0]));

            int from = randomIntBetween(0, numRelevantDocs);
            int size = randomIntBetween(10, 100);
            SearchResponse searchResponse = client().prepareSearch().setQuery(pqb).setTrackTotalHits(true).setSize(size).setFrom(from)
                    .setSearchType(DFS_QUERY_THEN_FETCH)
                    .get();

            long numHits = searchResponse.getHits().getTotalHits().value;
            assertThat(numHits, lessThanOrEqualTo((long) numRelevantDocs + pins.size()));

            // Check pins are sorted by increasing score, (unlike organic, there are no duplicate scores)
            float lastScore = Float.MAX_VALUE;
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (int hitNumber = 0; hitNumber < Math.min(hits.length, pins.size() - from); hitNumber++) {
                assertThat("Hit " + hitNumber + " in iter " + i + " wrong" + pins, hits[hitNumber].getScore(), lessThan(lastScore));
                lastScore = hits[hitNumber].getScore();
            }
            // Check that the pins appear in the requested order (globalHitNumber is cursor independent of from and size window used)
            int globalHitNumber = 0;
            for (String id : pins) {
                if (globalHitNumber < size && globalHitNumber >= from) {
                    assertThat("Hit " + globalHitNumber + " in iter " + i + " wrong" + pins, hits[globalHitNumber - from].getId(),
                            equalTo(id));
                }
                globalHitNumber++;
            }
            // Test the organic hits are sorted by text relevance
            boolean highScoresExhausted = false;
            for (; globalHitNumber < hits.length + from; globalHitNumber++) {
                if (globalHitNumber >= from) {
                    int id = Integer.parseInt(hits[globalHitNumber - from].getId());
                    if (id % 2 == 0) {
                        highScoresExhausted = true;
                    } else {
                        assertFalse("All odd IDs should have scored higher than even IDs in organic results", highScoresExhausted);
                    }
                }

            }

        }

    }

    public void testExplain() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1")
                        .field("analyzer", "whitespace").field("type", "text").endObject().endObject().endObject().endObject()));
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("field1", "the quick brown fox").get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "pinned").get();
        client().prepareIndex("test", "type1", "3").setSource("field1", "irrelevant").get();
        client().prepareIndex("test", "type1", "4").setSource("field1", "slow brown cat").get();
        refresh();

        PinnedQueryBuilder pqb = new PinnedQueryBuilder(QueryBuilders.matchQuery("field1", "the quick brown").operator(Operator.OR), "2");

        SearchResponse searchResponse = client().prepareSearch().setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(pqb)
                .setExplain(true).get();
        assertHitCount(searchResponse, 3);
        assertFirstHit(searchResponse, hasId("2"));
        assertSecondHit(searchResponse, hasId("1"));
        assertThirdHit(searchResponse, hasId("4"));

        Explanation pinnedExplanation = searchResponse.getHits().getAt(0).getExplanation();
        assertThat(pinnedExplanation, notNullValue());
        assertThat(pinnedExplanation.isMatch(), equalTo(true));
        assertThat(pinnedExplanation.getDetails().length, equalTo(1));
        assertThat(pinnedExplanation.getDetails()[0].isMatch(), equalTo(true));
        assertThat(pinnedExplanation.getDetails()[0].getDescription(), containsString("ConstantScore"));


    }

}
