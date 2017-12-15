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

package org.elasticsearch.action.search;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.equalTo;


public class InnerHitsCollapseIT extends ESIntegTestCase {


    public void testSimpleCollapse() throws Exception {
        final String index = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        final String type = randomAlphaOfLength(8);
        final String collapseField = randomAlphaOfLength(8);

        assertAcked(prepareCreate(index).addMapping(type,
            jsonBuilder().startObject()
                .startObject("properties")
                .startObject("title")
                .field("type", "text")
                .endObject()
                .startObject(collapseField)
                .field("type", "keyword")
                .field("doc_values", "true")
                .endObject()
                .endObject().endObject()));

        List<IndexRequestBuilder> requests = new ArrayList<>();
        long version1 = randomLongBetween(0, 1000);
        requests.add(client().prepareIndex(index, type, "1")
            .setVersionType(VersionType.EXTERNAL)
            .setVersion(version1)
            .setSource(jsonBuilder().startObject().field("title", "quick brown fox")
                .field(collapseField, "ck1").endObject()));
        long version2 = randomLongBetween(0, 1000);
        requests.add(client().prepareIndex(index, type, "2")
            .setVersionType(VersionType.EXTERNAL)
            .setVersion(version2)
            .setSource(jsonBuilder().startObject().field("title", "big gray elephant")
                .field(collapseField, "ck2").endObject()));
        long version3 = randomLongBetween(0, 1000);
        requests.add(client().prepareIndex(index, type, "3")
            .setVersionType(VersionType.EXTERNAL)
            .setVersion(version3)
            .setSource(jsonBuilder().startObject().field("title", "big gray mice")
                .field(collapseField, "ck1").endObject()));
        indexRandom(true, requests);

        List<String> storedFields = new ArrayList<>(2);
        storedFields.add("*");
        storedFields.add("_source");
        CollapseBuilder cBuild = new CollapseBuilder(collapseField).setInnerHits(
            new InnerHitBuilder()
                .setName(collapseField)
                .setVersion(true)
                .setTrackScores(true)
                .setStoredFieldNames(storedFields)
                .addSort(new FieldSortBuilder("_id").order(SortOrder.ASC))
                .setFrom(0)
                .setSize(22)
                .setIgnoreUnmapped(false)
        );
        SearchResponse response = client().prepareSearch(index)
            .setQuery(matchAllQuery())
            .setCollapse(cBuild)
            .setVersion(true)
            .addSort("_id", SortOrder.ASC)
            .get();
        assertNoFailures(response);
        assertHitCount(response, 3);
        assertSearchHit(response, 1, hasId("1"));
        assertEquals(response.getHits().getAt(0).getVersion(), version1);
        assertEquals(response.getHits().getAt(1).getVersion(), version2);

        assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
        SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get(collapseField);
        assertThat(innerHits.getTotalHits(), equalTo(2L));
        assertThat(innerHits.getHits().length, equalTo(2));
        assertThat(innerHits.getAt(0).getId(), equalTo("1"));
        assertEquals(innerHits.getAt(0).getVersion(), version1);
        assertEquals(innerHits.getAt(1).getVersion(), version3);
    }

}
