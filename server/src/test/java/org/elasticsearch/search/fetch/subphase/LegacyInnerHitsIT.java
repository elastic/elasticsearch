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

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collections;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

public class LegacyInnerHitsIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testInnerHitsWithIgnoreUnmapped() throws Exception {
        assertAcked(prepareCreate("index1")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id))
                .addMapping("parent_type", "nested_type", "type=nested")
                .addMapping("child_type", "_parent", "type=parent_type")
        );
        createIndex("index2");
        client().prepareIndex("index1", "parent_type", "1").setSource("nested_type", Collections.singletonMap("key", "value")).get();
        client().prepareIndex("index1", "child_type", "2").setParent("1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index2", "type", "3").setSource("key", "value").get();
        refresh();

        SearchResponse response = client().prepareSearch("index1", "index2")
                .setQuery(boolQuery()
                        .should(nestedQuery("nested_type", matchAllQuery(), ScoreMode.None).ignoreUnmapped(true)
                                .innerHit(new InnerHitBuilder().setIgnoreUnmapped(true)))
                        .should(termQuery("key", "value"))
                )
                .get();
        assertNoFailures(response);
        assertHitCount(response, 2);
        assertSearchHits(response, "1", "3");
    }

}
