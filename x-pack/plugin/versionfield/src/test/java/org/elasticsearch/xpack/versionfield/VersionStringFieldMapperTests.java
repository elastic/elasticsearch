/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.versionfield;

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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class VersionStringFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        // return pluginList(VersionFieldPlugin.class, LocalStateCompositeXPackPlugin.class, PainlessPlugin.class);
        // TODO PainlessPlugin loading doesn't work when test is run through "gradle check"
        return pluginList(VersionFieldPlugin.class, LocalStateCompositeXPackPlugin.class);
    }

    public String setUpIndex(String indexName) throws IOException {
        createIndex(
            indexName,
            Settings.builder().put("index.number_of_shards", 1).build(),
            "_doc",
            "version",
            "type=version",
            "foo",
            "type=keyword"
        );
        ensureGreen(indexName);

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
        client().admin().indices().prepareRefresh(indexName).get();
        return indexName;
    }

    public void testExactQueries() throws Exception {
        String indexName = "test";
        setUpIndex(indexName);

        // match
        SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", ("1.0.0"))).get();
        assertEquals(1, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", "1.4.0")).get();
        assertEquals(0, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", "1.3.0")).get();
        assertEquals(0, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", "1.3.0+build.1234567")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        // term
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.termQuery("version", "1.0.0")).get();
        assertEquals(1, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.termQuery("version", "1.4.0")).get();
        assertEquals(0, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.termQuery("version", "1.3.0")).get();
        assertEquals(0, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.termQuery("version", "1.3.0+build.1234567")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        // terms
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.termsQuery("version", "1.0.0", "1.3.0")).get();
        assertEquals(1, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.termsQuery("version", "1.4.0", "1.3.0+build.1234567")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        // phrase query (just for keyword compatibility)
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchPhraseQuery("version", "2.1.0-alpha.beta")).get();
        assertEquals(1, response.getHits().getTotalHits().value);
    }

    public void testRangeQueries() throws Exception {
        String indexName = setUpIndex("test");
        SearchResponse response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.rangeQuery("version").from("1.0.0").to("3.0.0"))
            .get();
        assertEquals(4, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("1.1.0").to("3.0.0")).get();
        assertEquals(3, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.rangeQuery("version").from("0.1.0").to("2.1.0-alpha.beta"))
            .get();
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
    }

    public void testPrefixQuery() throws IOException {
        String indexName = setUpIndex("test");
        // prefix
        SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "1")).get();
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
    }

    public void testSort() throws IOException {
        String indexName = setUpIndex("test");

        // sort based on version field
        SearchResponse response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .addSort("version", SortOrder.DESC)
            .get();
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
        String indexName = "test_wildcard";
        createIndex(
            indexName,
            Settings.builder().put("index.number_of_shards", 1).build(),
            "_doc",
            "version",
            "type=version",
            "foo",
            "type=keyword"
        );
        ensureGreen(indexName);

        client().prepareIndex(indexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field("version", "1.0.0-alpha.2.1.0-rc.1").endObject())
            .get();
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
        client().admin().indices().prepareRefresh(indexName).get();

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

    public void testPreReleaseFlag() throws IOException {
        String indexName = setUpIndex("test");
        SearchResponse response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery("version.isPreRelease", true)))
            .get();
        assertEquals(1, response.getHits().getTotalHits().value);
    }

    public void testMainVersionParts() throws IOException {
        String indexName = setUpIndex("test");
        SearchResponse response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery("version.major", 11)))
            .get();
        assertEquals(1, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery("version.minor", 1)))
            .get();
        assertEquals(3, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery("version.patch", 0)))
            .get();
        assertEquals(6, response.getHits().getTotalHits().value);
    }

    public void testStoreMalformed() throws Exception {
        String indexName = "test_malformed";
        createIndex(
            indexName,
            Settings.builder().put("index.number_of_shards", 1).build(),
            "_doc",
            "version",
            "type=version,store_malformed=true"
        );
        ensureGreen(indexName);

        client().prepareIndex(indexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field("version", "1.invalid.0").endObject())
            .get();
        client().prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().field("version", "2.2.0").endObject()).get();
        client().prepareIndex(indexName)
            .setId("3")
            .setSource(jsonBuilder().startObject().field("version", "2.2.0-badchar!").endObject())
            .get();
        client().admin().indices().prepareRefresh(indexName).get();

        SearchResponse response = client().prepareSearch(indexName).addDocValueField("version").get();
        assertEquals(3, response.getHits().getTotalHits().value);
        assertEquals("1", response.getHits().getAt(0).getId());
        assertEquals("1.invalid.0", response.getHits().getAt(0).field("version").getValue());

        assertEquals("2", response.getHits().getAt(1).getId());
        assertEquals("2.2.0", response.getHits().getAt(1).field("version").getValue());

        assertEquals("3", response.getHits().getAt(2).getId());
        assertEquals("2.2.0-badchar!", response.getHits().getAt(2).field("version").getValue());

        // exact match for malformed term
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", "1.invalid.0")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", "2.2.0-badchar!")).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        // also should appear in terms aggs
        response = client().prepareSearch(indexName).addAggregation(AggregationBuilders.terms("myterms").field("version")).get();
        Terms terms = response.getAggregations().get("myterms");
        List<? extends Bucket> buckets = terms.getBuckets();

        assertEquals(3, buckets.size());
        assertEquals("2.2.0", buckets.get(0).getKey());
        assertEquals("1.invalid.0", buckets.get(1).getKey());
        assertEquals("2.2.0-badchar!", buckets.get(2).getKey());

        // invalid values should sort after all valid ones
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).addSort("version", SortOrder.ASC).get();
        assertEquals(3, response.getHits().getTotalHits().value);
        SearchHit[] hits = response.getHits().getHits();
        assertEquals("2.2.0", hits[0].getSortValues()[0]);
        assertEquals("1.invalid.0", hits[1].getSortValues()[0]);
        assertEquals("2.2.0-badchar!", hits[2].getSortValues()[0]);

        // ranges can include them, but they are sorted last
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").to("3.0.0")).get();
        assertEquals(1, response.getHits().getTotalHits().value);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("3.0.0")).get();
        assertEquals(2, response.getHits().getTotalHits().value);
    }

    public void testAggs() throws Exception {
        String indexName = "test_aggs";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).build(), "_doc", "version", "type=version");
        ensureGreen(indexName);

        client().prepareIndex(indexName).setId("1").setSource(jsonBuilder().startObject().field("version", "1.0").endObject()).get();
        client().prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().field("version", "1.3.0").endObject()).get();
        client().prepareIndex(indexName)
            .setId("3")
            .setSource(jsonBuilder().startObject().field("version", "2.1.0-alpha").endObject())
            .get();
        client().prepareIndex(indexName).setId("4").setSource(jsonBuilder().startObject().field("version", "2.1.0").endObject()).get();
        client().prepareIndex(indexName).setId("5").setSource(jsonBuilder().startObject().field("version", "3.11.5").endObject()).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // terms aggs
        SearchResponse response = client().prepareSearch(indexName)
            .addAggregation(AggregationBuilders.terms("myterms").field("version"))
            .get();
        Terms terms = response.getAggregations().get("myterms");
        List<? extends Bucket> buckets = terms.getBuckets();

        assertEquals(5, buckets.size());
        assertEquals("1.0", buckets.get(0).getKey());
        assertEquals("1.3.0", buckets.get(1).getKey());
        assertEquals("2.1.0-alpha", buckets.get(2).getKey());
        assertEquals("2.1.0", buckets.get(3).getKey());
        assertEquals("3.11.5", buckets.get(4).getKey());

        // test terms aggs on version parts

        response = client().prepareSearch(indexName).addAggregation(AggregationBuilders.terms("myterms").field("version.major")).get();
        terms = response.getAggregations().get("myterms");
        buckets = terms.getBuckets();
        assertEquals(3, buckets.size());
        assertEquals(1L, buckets.get(0).getKey());
        assertEquals(2L, buckets.get(0).getDocCount());
        assertEquals(2L, buckets.get(1).getKey());
        assertEquals(2L, buckets.get(1).getDocCount());
        assertEquals(3L, buckets.get(2).getKey());
        assertEquals(1L, buckets.get(2).getDocCount());

        response = client().prepareSearch(indexName).addAggregation(AggregationBuilders.terms("myterms").field("version.minor")).get();
        terms = response.getAggregations().get("myterms");
        buckets = terms.getBuckets();
        assertEquals(4, buckets.size());
        assertEquals(1L, buckets.get(0).getKey());
        assertEquals(2L, buckets.get(0).getDocCount());
        assertEquals(0L, buckets.get(1).getKey());
        assertEquals(1L, buckets.get(1).getDocCount());
        assertEquals(3L, buckets.get(2).getKey());
        assertEquals(1L, buckets.get(2).getDocCount());
        assertEquals(11L, buckets.get(3).getKey());
        assertEquals(1L, buckets.get(3).getDocCount());

        response = client().prepareSearch(indexName).addAggregation(AggregationBuilders.terms("myterms").field("version.patch")).get();
        terms = response.getAggregations().get("myterms");
        buckets = terms.getBuckets();
        assertEquals(2, buckets.size());
        assertEquals(0L, buckets.get(0).getKey());
        assertEquals(3L, buckets.get(0).getDocCount());
        assertEquals(5L, buckets.get(1).getKey());
        assertEquals(1L, buckets.get(1).getDocCount());
    }

    public void testNullValue() throws Exception {
        String indexName = "test_nullvalue";
        createIndex(
            indexName,
            Settings.builder().put("index.number_of_shards", 1).build(),
            "_doc",
            "version",
            "type=version,null_value=0.0.0"
        );
        ensureGreen(indexName);

        client().prepareIndex(indexName).setId("1").setSource(jsonBuilder().startObject().field("version", "2.2.0").endObject()).get();
        client().prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().nullField("version").endObject()).get();
        client().admin().indices().prepareRefresh(indexName).get();

        SearchResponse response = client().prepareSearch(indexName).addDocValueField("version").addSort("version", SortOrder.ASC).get();
        assertEquals(2, response.getHits().getTotalHits().value);
        assertEquals("2", response.getHits().getAt(0).getId());
        assertEquals("0.0.0", response.getHits().getAt(0).field("version").getValue());

        assertEquals("1", response.getHits().getAt(1).getId());
        assertEquals("2.2.0", response.getHits().getAt(1).field("version").getValue());

        // range
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").to("3.0.0")).get();
        assertEquals(2, response.getHits().getTotalHits().value);
    }

    // TODO how to test scripts in ESSingleNodeTestCase?
    // Currently fails with "java.lang.IllegalArgumentException: Illegal list shortcut value [value]".

    // public void testScripting() throws Exception {
    // String indexName = setUpIndex("test");
    //
    // ScriptQueryBuilder query = QueryBuilders.scriptQuery(new Script("doc['version'].value.length() <= 5"));
    // SearchResponse response = client().prepareSearch(indexName).addDocValueField("version").setQuery(query).get();
    // assertEquals(2, response.getHits().getTotalHits().value);
    // assertEquals("2", response.getHits().getAt(0).getId());
    // assertEquals("1.0.0", response.getHits().getAt(0).field("version").getValue());
    //
    // assertEquals("5", response.getHits().getAt(1).getId());
    // assertEquals("2.1.0", response.getHits().getAt(1).field("version").getValue());
    // }
}
