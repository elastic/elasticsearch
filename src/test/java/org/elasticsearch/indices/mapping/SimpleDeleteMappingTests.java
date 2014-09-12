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

import com.google.common.base.Predicate;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class SimpleDeleteMappingTests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleDeleteMapping() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", "value", "type=string").execute().actionGet());
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("value", "test" + i)
                    .endObject()).execute().actionGet();
        }

        ensureGreen();
        refresh();

        for (int i = 0; i < 10; i++) {
            CountResponse countResponse = client().prepareCount().setQuery(matchAllQuery()).execute().actionGet();
            assertThat(countResponse.getCount(), equalTo(10l));
        }

        waitForMappingOnMaster("test", "type1");

        GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings("test").setTypes("type1").execute().actionGet();
        assertThat(mappingsResponse.getMappings().get("test").get("type1"), notNullValue());

        assertAcked(client().admin().indices().prepareDeleteMapping().setIndices("test").setType("type1"));

        for (int i = 0; i < 10; i++) {
            CountResponse countResponse = client().prepareCount().setQuery(matchAllQuery()).execute().actionGet();
            assertThat(countResponse.getCount(), equalTo(0l));
        }
        assertBusy(new Runnable() {
            @Override
            public void run() {
                GetMappingsResponse response = client().admin().indices().prepareGetMappings().get();
                assertTrue(response.getMappings().containsKey("test"));
                assertFalse(response.getMappings().get("test").containsKey("type1"));
            }
        });
    }
    
    
    @Test
    public void deleteMappingAllowNoBlankIndexAndNoEmptyStrings() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("index1").addMapping("1", "field1", "type=string").get());
        assertAcked(client().admin().indices().prepareCreate("1index").addMapping("1", "field1", "type=string").get());

        // Should succeed, since no wildcards
        client().admin().indices().prepareDeleteMapping("1index").setType("1").get();
        try {
            client().admin().indices().prepareDeleteMapping("_all").get();
            fail();
        } catch (ActionRequestValidationException e) {}
        
        try {
            client().admin().indices().prepareDeleteMapping("_all").setType("").get();
            fail();
        } catch (ActionRequestValidationException e) {}
        
        try {
            client().admin().indices().prepareDeleteMapping().setType("1").get();
            fail();
        } catch (ActionRequestValidationException e) {}
        
        try {
            client().admin().indices().prepareDeleteMapping("").setType("1").get();
            fail();
        } catch (ActionRequestValidationException e) {}

    }
    
}
