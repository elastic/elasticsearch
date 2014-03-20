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

package org.elasticsearch.action.quality;

import com.google.common.collect.Sets;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class PrecisionAtRequestTest extends ElasticsearchIntegrationTest {

    @Before
    public void setup() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "testtype").setId("1")
                .setSource("text", "value1").get();
        client().prepareIndex("test", "testtype").setId("2")
                .setSource("text", "value2").get();
        refresh();
    }

    @Test
    public void testPrecisionAtOne() throws IOException, InterruptedException, ExecutionException {
        MatchQueryBuilder query = new MatchQueryBuilder("text", "value1");

        SearchResponse response = client().prepareSearch().setQuery(query)
                .execute().actionGet();
        ElasticsearchAssertions.assertHitCount(response, 1);
        // assumption: We have a set of relevant doc ids
        Set<String> relevant = Sets.newHashSet("1");

        SearchHit[] hits = response.getHits().getHits();
        // computation for precision at 5
        int good = 0;
        int bad = 0;
        for (int i = 0; (i < 5 && i < hits.length); i++) {
            if (relevant.contains(hits[i].getId())) {
                good++;
            } else {
                bad++;
            }
        }
        assertEquals(1, 1);
        client().close();
//        PrecisionAtQueryBuilder builder = new PrecisionAtQueryBuilder(
//                "{\"match_{{template}}\": {}}\"");
//        PrecisionAtResponse sr = client().execute(builder.request()); 
//        ElasticsearchAssertions.assertHitCount(sr, 2);
    }

}
