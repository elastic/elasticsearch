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

package org.elasticsearch.indices.mapping;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.emptyIterable;

@ESIntegTestCase.ClusterScope(randomDynamicTemplates = false) // this test takes a long time to delete the idx if all fields are eager loading
public class ConcurrentDynamicTemplateIT extends ESIntegTestCase {

    private final String mappingType = "test-mapping";

    @Test // see #3544
    public void testConcurrentDynamicMapping() throws Exception {
        final String fieldName = "field";
        final String mapping = "{ \"" + mappingType + "\": {" +
                "\"dynamic_templates\": ["
                + "{ \"" + fieldName + "\": {" + "\"path_match\": \"*\"," + "\"mapping\": {" + "\"type\": \"string\"," + "\"store\": \"yes\","
                + "\"index\": \"analyzed\", \"analyzer\": \"whitespace\" } } } ] } }";
        // The 'fieldNames' array is used to help with retrieval of index terms
        // after testing

        int iters = scaledRandomIntBetween(5, 15);
        for (int i = 0; i < iters; i++) {
            cluster().wipeIndices("test");
            assertAcked(prepareCreate("test")
                    .addMapping(mappingType, mapping));
            ensureYellow();
            int numDocs = scaledRandomIntBetween(10, 100);
            final CountDownLatch latch = new CountDownLatch(numDocs);
            final List<Throwable> throwable = new CopyOnWriteArrayList<>();
            int currentID = 0;
            for (int j = 0; j < numDocs; j++) {
                Map<String, Object> source = new HashMap<>();
                source.put(fieldName, "test-user");
                client().prepareIndex("test", mappingType, Integer.toString(currentID++)).setSource(source).execute(new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse response) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        throwable.add(e);
                        latch.countDown();
                    }
                });
            }
            latch.await();
            assertThat(throwable, emptyIterable());
            refresh();
            assertHitCount(client().prepareSearch("test").setQuery(QueryBuilders.matchQuery(fieldName, "test-user")).get(), numDocs);
            assertHitCount(client().prepareSearch("test").setQuery(QueryBuilders.matchQuery(fieldName, "test user")).get(), 0);

        }
    }
}