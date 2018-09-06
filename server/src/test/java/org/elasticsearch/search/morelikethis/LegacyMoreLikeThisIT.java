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

package org.elasticsearch.search.morelikethis;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class LegacyMoreLikeThisIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testSimpleMoreLikeThisIdsMultipleTypes() throws Exception {
        logger.info("Creating index test");
        int numOfTypes = randomIntBetween(2, 10);
        CreateIndexRequestBuilder createRequestBuilder = prepareCreate("test")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id));
        for (int i = 0; i < numOfTypes; i++) {
            createRequestBuilder.addMapping("type" + i, jsonBuilder().startObject().startObject("type" + i).startObject("properties")
                    .startObject("text").field("type", "text").endObject()
                    .endObject().endObject().endObject());
        }
        assertAcked(createRequestBuilder);

        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        List<IndexRequestBuilder> builders = new ArrayList<>(numOfTypes);
        for (int i = 0; i < numOfTypes; i++) {
            builders.add(client().prepareIndex("test", "type" + i).setSource("text", "lucene" + " " + i).setId(String.valueOf(i)));
        }
        indexRandom(true, builders);

        logger.info("Running MoreLikeThis");
        MoreLikeThisQueryBuilder queryBuilder =
                QueryBuilders
                        .moreLikeThisQuery(
                                new String[]{"text"},
                                null,
                                new MoreLikeThisQueryBuilder.Item[]{new MoreLikeThisQueryBuilder.Item("test", "type0", "0")})
                        .include(true)
                        .minTermFreq(1)
                        .minDocFreq(1);

        String[] types = new String[numOfTypes];
        for (int i = 0; i < numOfTypes; i++) {
            types[i] = "type" + i;
        }
        SearchResponse mltResponse = client().prepareSearch().setTypes(types).setQuery(queryBuilder).execute().actionGet();
        assertHitCount(mltResponse, numOfTypes);
    }

}
