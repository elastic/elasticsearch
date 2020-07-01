/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.versionfield.VersionFieldPlugin;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class VersionStringFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(VersionFieldPlugin.class, LocalStateCompositeXPackPlugin.class);
    }

    public void testQueries() throws Exception {
        String indexName = "test";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).build(), "_doc", "version", "type=version", "foo", "type=keyword");
        ensureGreen("test");

        client().prepareIndex(indexName).setId("1").setSource(jsonBuilder().startObject().field("version", "11.1.0").endObject()).get();
        client().prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().field("version", "1.0.0").endObject()).get();
        client().prepareIndex(indexName)
            .setId("3")
            .setSource(jsonBuilder().startObject().field("version", "1.3.0+build.1234567").endObject())
            .get();
        client().prepareIndex(indexName)
            .setId("4")
            .setSource(jsonBuilder().startObject().field("version", "2.1.0-alpha.beta").endObject())
            .get();
        client().prepareIndex(indexName).setId("5").setSource(jsonBuilder().startObject().field("version", "2.1.0").endObject()).get();
        client().prepareIndex(indexName).setId("6").setSource(jsonBuilder().startObject().field("version", "21.11.0").endObject()).get();
        client().admin().indices().prepareRefresh("test").get();

        SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", ("1.0.0"))).get();
        assertEquals(1, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", ("1.4.0"))).get();
        assertEquals(0, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", ("1.3.0"))).get();
        assertEquals(0, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", ("1.3.0+build.1234567"))).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        // ranges
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("1.0.0").to("3.0.0")).get();
        assertEquals(4, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("1.1.0").to("3.0.0")).get();
        assertEquals(3, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("0.1.0").to("2.1.0-alpha.beta")).get();
        assertEquals(3, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("2.1.0").to("3.0.0")).get();
        assertEquals(1, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("3.0.0").to("4.0.0")).get();
        assertEquals(0, response.getHits().getTotalHits().value);

        // ranges excluding edges
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("1.0.0", false).to("3.0.0")).get();
        assertEquals(3, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("1.0.0").to("2.1.0", false)).get();
        assertEquals(3, response.getHits().getTotalHits().value);

        // open ranges
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("1.4.0")).get();
        assertEquals(4, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").to("1.4.0")).get();
        assertEquals(2, response.getHits().getTotalHits().value);

        // prefix
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "1")).get();
        assertEquals(3, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "2.1")).get();
        assertEquals(2, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "2.1.0-")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "1.3.0+b")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "2")).get();
        assertEquals(3, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "21")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "21.")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "21.1")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "21.11")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        // sort based on version field
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).addSort("version", SortOrder.DESC).get();
        assertEquals(6, response.getHits().getTotalHits().value);
        SearchHit[] hits = response.getHits().getHits();
        assertEquals("21.11.0", hits[0].getSortValues()[0]);
        assertEquals("11.1.0", hits[1].getSortValues()[0]);
        assertEquals("2.1.0", hits[2].getSortValues()[0]);
        assertEquals("2.1.0-alpha.beta", hits[3].getSortValues()[0]);
        assertEquals("1.3.0+build.1234567", hits[4].getSortValues()[0]);
        assertEquals("1.0.0", hits[5].getSortValues()[0]);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).addSort("version", SortOrder.ASC).get();
        assertEquals(6, response.getHits().getTotalHits().value);
        hits = response.getHits().getHits();
        assertEquals("1.0.0", hits[0].getSortValues()[0]);
        assertEquals("1.3.0+build.1234567", hits[1].getSortValues()[0]);
        assertEquals("2.1.0-alpha.beta", hits[2].getSortValues()[0]);
        assertEquals("2.1.0", hits[3].getSortValues()[0]);
        assertEquals("11.1.0", hits[4].getSortValues()[0]);
        assertEquals("21.11.0", hits[5].getSortValues()[0]);
    }

    public void testWildcardQuery() throws Exception {
        String indexName = "test";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).build(), "_doc", "version", "type=version", "foo", "type=keyword");
        ensureGreen("test");

        client().prepareIndex(indexName).setId("1").setSource(jsonBuilder().startObject().field("version", "1.0.0-alpha.2.1.0-rc.1").endObject()).get();
        client().prepareIndex(indexName)
            .setId("2")
            .setSource(jsonBuilder().startObject().field("version", "1.3.0+build.1234567").endObject())
            .get();
        client().prepareIndex(indexName)
            .setId("3")
            .setSource(jsonBuilder().startObject().field("version", "2.1.0-alpha.beta").endObject())
            .get();
        client().prepareIndex(indexName).setId("4").setSource(jsonBuilder().startObject().field("version", "2.1.0").endObject()).get();
        client().prepareIndex(indexName).setId("5").setSource(jsonBuilder().startObject().field("version", "2.33.0").endObject()).get();
        client().admin().indices().prepareRefresh("test").get();

        // wildcard
        SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", "*alpha*")).get();
        assertEquals(2, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", "*b*")).get();
        assertEquals(2, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", "*bet*")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", "2.1*")).get();
        assertEquals(2, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", "2.1.0-*")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", "*2.1.0-*")).get();
        assertEquals(2, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", "*2.1.0*")).get();
        assertEquals(3, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", "*2.?.0*")).get();
        assertEquals(3, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", "*2.??.0*")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", "?.1.0")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", "*-*")).get();
        assertEquals(2, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", "1.3.0+b*")).get();
        assertEquals(1, response.getHits().getTotalHits().value);
    }

    public void testIgnoreMalformed() throws Exception {
        String indexName = "test_malformed";
        createIndex(
            indexName,
            Settings.builder().put("index.number_of_shards", 1).build(),
            "_doc",
            "version",
            "type=version,ignore_malformed=true"
        );
        ensureGreen(indexName);

        client().prepareIndex(indexName).setId("1").setSource(jsonBuilder().startObject().field("version", "1...0.0").endObject()).get();
        client().prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().field("version", "1.2.0").endObject()).get();
        client().admin().indices().prepareRefresh(indexName).get();

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
        String indexName = "test_aggs";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).build(), "_doc", "version", "type=version");
        ensureGreen(indexName);

        client().prepareIndex(indexName).setId("1").setSource(jsonBuilder().startObject().field("version", "1.0.0").endObject()).get();
        client().prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().field("version", "1.3.0").endObject()).get();
        client().prepareIndex(indexName)
            .setId("3")
            .setSource(jsonBuilder().startObject().field("version", "2.1.0-alpha").endObject())
            .get();
        client().prepareIndex(indexName).setId("4").setSource(jsonBuilder().startObject().field("version", "2.1.0").endObject()).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // range aggs
        SearchResponse response = client().prepareSearch(indexName)
            .addAggregation(AggregationBuilders.terms("myterms").field("version"))
            .get();
        Terms terms = response.getAggregations().get("myterms");
        List<? extends Bucket> buckets = terms.getBuckets();
        ;
        assertEquals(4, buckets.size());
        assertEquals("1.0.0", buckets.get(0).getKey());
        assertEquals("1.3.0", buckets.get(1).getKey());
        assertEquals("2.1.0-alpha", buckets.get(2).getKey());
        assertEquals("2.1.0", buckets.get(3).getKey());
    }
}
