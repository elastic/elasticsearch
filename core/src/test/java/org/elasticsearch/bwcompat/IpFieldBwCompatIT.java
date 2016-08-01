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
package org.elasticsearch.bwcompat;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

import java.util.Collection;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

@ESIntegTestCase.SuiteScopeTestCase
public class IpFieldBwCompatIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(InternalSettingsPlugin.class); // uses index.merge.enabled
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("old_index")
                .setSettings(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_3_3.id)
                .addMapping("type", "ip_field", "type=ip"));
        assertAcked(prepareCreate("new_index")
                .addMapping("type", "ip_field", "type=ip"));

        indexRandom(true,
                client().prepareIndex("old_index", "type", "1").setSource("ip_field", "127.0.0.1"),
                client().prepareIndex("new_index", "type", "1").setSource("ip_field", "127.0.0.1"),
                client().prepareIndex("new_index", "type", "2").setSource("ip_field", "::1"));
    }

    public void testSort() {
        SearchResponse response = client().prepareSearch("old_index", "new_index")
                .addSort(SortBuilders.fieldSort("ip_field")).get();
        assertNoFailures(response);
        assertEquals(3, response.getHits().totalHits());
        assertEquals("::1", response.getHits().getAt(0).getSortValues()[0]);
        assertEquals("127.0.0.1", response.getHits().getAt(1).getSortValues()[0]);
        assertEquals("127.0.0.1", response.getHits().getAt(2).getSortValues()[0]);
    }

    public void testRangeAgg() {
        SearchResponse response = client().prepareSearch("old_index", "new_index")
                .addAggregation(AggregationBuilders.ipRange("ip_range").field("ip_field")
                        .addMaskRange("127.0.0.1/16")
                        .addMaskRange("::1/64")).get();
        assertNoFailures(response);
        assertEquals(3, response.getHits().totalHits());
        Range range = response.getAggregations().get("ip_range");
        assertEquals(2, range.getBuckets().size());
        assertEquals("::1/64", range.getBuckets().get(0).getKeyAsString());
        assertEquals(3, range.getBuckets().get(0).getDocCount());
        assertEquals("127.0.0.1/16", range.getBuckets().get(1).getKeyAsString());
        assertEquals(2, range.getBuckets().get(1).getDocCount());
    }

    public void testTermsAgg() {
        SearchResponse response = client().prepareSearch("old_index", "new_index")
                .addAggregation(AggregationBuilders.terms("ip_terms").field("ip_field")).get();
        assertNoFailures(response);
        assertEquals(3, response.getHits().totalHits());
        Terms terms = response.getAggregations().get("ip_terms");
        assertEquals(2, terms.getBuckets().size());
        assertEquals(2, terms.getBucketByKey("127.0.0.1").getDocCount());
        assertEquals(1, terms.getBucketByKey("::1").getDocCount());
    }
}
