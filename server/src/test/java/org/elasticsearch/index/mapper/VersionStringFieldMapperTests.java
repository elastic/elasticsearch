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

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class VersionStringFieldMapperTests extends ESSingleNodeTestCase {

    public void testQueries() throws Exception {
        String indexName = "test";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).build(), "_doc",
            "version", "type=version");
        ensureGreen("test");

        client().prepareIndex(indexName).setId("1")
            .setSource(jsonBuilder().startObject().field("version", "1.0.0").endObject())
            .get();
        client().prepareIndex(indexName).setId("2")
            .setSource(jsonBuilder().startObject().field("version", "1.3.0+build1234567").endObject())
        .get();
        client().prepareIndex(indexName).setId("3")
        .setSource(jsonBuilder().startObject().field("version", "2.1.0-alpha").endObject())
            .get();
        client().prepareIndex(indexName).setId("4")
            .setSource(jsonBuilder().startObject().field("version", "2.1.0").endObject())
        .get();
        client().admin().indices().prepareRefresh("test").get();

        SearchResponse response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchQuery("version", ("1.0.0"))).get();
        assertEquals(1, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchQuery("version", ("1.4.0"))).get();
        assertEquals(0, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchQuery("version", ("1.3.0"))).get();
        assertEquals(0, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchQuery("version", ("1.3.0+build1234567"))).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        // ranges
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.rangeQuery("version").from("1.0.0").to("3.0.0"))
            .get();
        assertEquals(4, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.rangeQuery("version").from("1.1.0").to("3.0.0"))
            .get();
        assertEquals(3, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.rangeQuery("version").from("0.1.0").to("2.1.0-alpha"))
            .get();
        assertEquals(3, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.rangeQuery("version").from("2.1.0").to("3.0.0"))
            .get();
        assertEquals(1, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.rangeQuery("version").from("3.0.0").to("4.0.0"))
            .get();
        assertEquals(0, response.getHits().getTotalHits().value);

        // ranges excluding edges
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.rangeQuery("version").from("1.0.0", false).to("3.0.0"))
            .get();
        assertEquals(3, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.rangeQuery("version").from("1.0.0").to("2.1.0", false))
            .get();
        assertEquals(3, response.getHits().getTotalHits().value);

        // open ranges
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.rangeQuery("version").from("1.4.0"))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.rangeQuery("version").to("1.4.0"))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);

        // prefix won't work, would need to encode this differently
//        response = client().prepareSearch(indexName)
//            .setQuery(QueryBuilders.prefixQuery("version","1"))
//            .get();
//        assertEquals(2, response.getHits().getTotalHits().value);
//
//        response = client().prepareSearch(indexName)
//            .setQuery(QueryBuilders.prefixQuery("version","2.0"))
//            .get();
//        assertEquals(1, response.getHits().getTotalHits().value);

        // sort based on version field
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery()).addSort("version", SortOrder.DESC).get();
        assertEquals(4, response.getHits().getTotalHits().value);
        SearchHit[] hits = response.getHits().getHits();
        assertEquals("2.1.0", hits[0].getSortValues()[0]);
        assertEquals("2.1.0-alpha", hits[1].getSortValues()[0]);
        assertEquals("1.3.0+build1234567", hits[2].getSortValues()[0]);
        assertEquals("1.0.0", hits[3].getSortValues()[0]);
    }

    public void testIgnoreMalformed() throws Exception {
        String indexName = "test";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).build(), "_doc",
            "version", "type=version,ignore_malformed=true");
        ensureGreen("test");

        client().prepareIndex(indexName).setId("1")
            .setSource(jsonBuilder().startObject().field("version", "1...0.0").endObject())
            .get();
        client().prepareIndex(indexName).setId("2")
        .setSource(jsonBuilder().startObject().field("version", "1.2.0").endObject())
        .get();
        client().admin().indices().prepareRefresh("test").get();

        SearchResponse response = client().prepareSearch(indexName).addDocValueField("version").get();
        assertEquals(2, response.getHits().getTotalHits().value);
        assertEquals("1", response.getHits().getAt(0).getId());
        assertEquals("version", response.getHits().getAt(0).field("_ignored").getValue());
        assertNull(response.getHits().getAt(0).field("version").getValue());

        assertEquals("2", response.getHits().getAt(1).getId());
        assertNull(response.getHits().getAt(1).field("_ignored"));
        assertEquals("1.2.0", response.getHits().getAt(1).field("version").getValue());
    }

    public void testAggs() throws Exception {
        String indexName = "test";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).build(), "_doc",
            "version", "type=version");
        ensureGreen("test");

        client().prepareIndex(indexName).setId("1")
            .setSource(jsonBuilder().startObject().field("version", "1.0.0").endObject())
            .get();
        client().prepareIndex(indexName).setId("2")
            .setSource(jsonBuilder().startObject().field("version", "1.3.0").endObject())
        .get();
        client().prepareIndex(indexName).setId("3")
        .setSource(jsonBuilder().startObject().field("version", "2.1.0-alpha").endObject())
            .get();
        client().prepareIndex(indexName).setId("4")
            .setSource(jsonBuilder().startObject().field("version", "2.1.0").endObject())
        .get();
        client().admin().indices().prepareRefresh("test").get();

        // range aggs
        SearchResponse response = client().prepareSearch(indexName)
            .addAggregation(AggregationBuilders.terms("myterms").field("version")).get();
        Terms terms = response.getAggregations().get("myterms");
        List<? extends Bucket> buckets = terms.getBuckets();;
        assertEquals(4, buckets.size());
        assertEquals("1.0.0", buckets.get(0).getKey());
        assertEquals("1.3.0", buckets.get(1).getKey());
        assertEquals("2.1.0-alpha", buckets.get(2).getKey());
        assertEquals("2.1.0", buckets.get(3).getKey());
    }
}
