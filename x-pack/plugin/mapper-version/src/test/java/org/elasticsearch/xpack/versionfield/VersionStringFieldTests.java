/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.stringstats.InternalStringStats;
import org.elasticsearch.xpack.analytics.stringstats.StringStatsAggregationBuilder;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class VersionStringFieldTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(VersionFieldPlugin.class, LocalStateCompositeXPackPlugin.class, AnalyticsPlugin.class);
    }

    public String setUpIndex(String indexName) throws IOException {
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).build(), "_doc", "version", "type=version");
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1").setSource(jsonBuilder().startObject().field("version", "11.1.0").endObject()).get();
        prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().field("version", "1.0.0").endObject()).get();
        prepareIndex(indexName).setId("3").setSource(jsonBuilder().startObject().field("version", "1.3.0+build.1234567").endObject()).get();
        prepareIndex(indexName).setId("4").setSource(jsonBuilder().startObject().field("version", "2.1.0-alpha.beta").endObject()).get();
        prepareIndex(indexName).setId("5").setSource(jsonBuilder().startObject().field("version", "2.1.0").endObject()).get();
        prepareIndex(indexName).setId("6").setSource(jsonBuilder().startObject().field("version", "21.11.0").endObject()).get();
        client().admin().indices().prepareRefresh(indexName).get();
        return indexName;
    }

    public void testExactQueries() throws Exception {
        String indexName = "test";
        setUpIndex(indexName);

        // match
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", ("1.0.0"))), 1);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", "1.4.0")), 0);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", "1.3.0")), 0);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", "1.3.0+build.1234567")), 1);

        // term
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.termQuery("version", "1.0.0")), 1);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.termQuery("version", "1.4.0")), 0);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.termQuery("version", "1.3.0")), 0);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.termQuery("version", "1.3.0+build.1234567")), 1);

        // terms
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.termsQuery("version", "1.0.0", "1.3.0")), 1);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.termsQuery("version", "1.4.0", "1.3.0+build.1234567")), 1);

        // phrase query (just for keyword compatibility)
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.matchPhraseQuery("version", "2.1.0-alpha.beta")), 1);
    }

    public void testRangeQueries() throws Exception {
        String indexName = setUpIndex("test");
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("1.0.0").to("3.0.0")), 4);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("1.1.0").to("3.0.0")), 3);
        assertHitCount(
            client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("0.1.0").to("2.1.0-alpha.beta")),
            3
        );
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("2.1.0").to("3.0.0")), 1);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("3.0.0").to("4.0.0")), 0);
        assertHitCount(
            client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("1.3.0+build.1234569").to("3.0.0")),
            2
        );

        // ranges excluding edges
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("1.0.0", false).to("3.0.0")), 3);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("1.0.0").to("2.1.0", false)), 3);

        // open ranges
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("1.4.0")), 4);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").to("1.4.0")), 2);
    }

    public void testPrefixQuery() throws IOException {
        String indexName = setUpIndex("test");
        // prefix
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "1")), 3);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "2.1")), 2);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "2.1.0-")), 1);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "1.3.0+b")), 1);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "2")), 3);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "21")), 1);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "21.")), 1);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "21.1")), 1);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "21.11")), 1);

        // test case sensitivity / insensitivity
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "2.1.0-A")), 0);
        assertResponse(
            client().prepareSearch(indexName).setQuery(QueryBuilders.prefixQuery("version", "2.1.0-A").caseInsensitive(true)),
            response -> {
                assertEquals(1, response.getHits().getTotalHits().value());
                assertEquals("2.1.0-alpha.beta", response.getHits().getHits()[0].getSourceAsMap().get("version"));
            }
        );
    }

    public void testSort() throws IOException {
        String indexName = setUpIndex("test");
        // also adding some invalid versions that should be sorted after legal ones
        prepareIndex(indexName).setSource(jsonBuilder().startObject().field("version", "1.2.3alpha").endObject()).get();
        prepareIndex(indexName).setSource(jsonBuilder().startObject().field("version", "1.3.567#12").endObject()).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // sort based on version field
        assertResponse(
            client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).addSort("version", SortOrder.DESC),
            response -> {
                assertEquals(8, response.getHits().getTotalHits().value());
                SearchHit[] hits = response.getHits().getHits();
                assertEquals("1.3.567#12", hits[0].getSortValues()[0]);
                assertEquals("1.2.3alpha", hits[1].getSortValues()[0]);
                assertEquals("21.11.0", hits[2].getSortValues()[0]);
                assertEquals("11.1.0", hits[3].getSortValues()[0]);
                assertEquals("2.1.0", hits[4].getSortValues()[0]);
                assertEquals("2.1.0-alpha.beta", hits[5].getSortValues()[0]);
                assertEquals("1.3.0+build.1234567", hits[6].getSortValues()[0]);
                assertEquals("1.0.0", hits[7].getSortValues()[0]);
            }
        );

        assertResponse(
            client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).addSort("version", SortOrder.ASC),
            response -> {
                assertEquals(8, response.getHits().getTotalHits().value());
                var hits = response.getHits().getHits();
                assertEquals("1.0.0", hits[0].getSortValues()[0]);
                assertEquals("1.3.0+build.1234567", hits[1].getSortValues()[0]);
                assertEquals("2.1.0-alpha.beta", hits[2].getSortValues()[0]);
                assertEquals("2.1.0", hits[3].getSortValues()[0]);
                assertEquals("11.1.0", hits[4].getSortValues()[0]);
                assertEquals("21.11.0", hits[5].getSortValues()[0]);
                assertEquals("1.2.3alpha", hits[6].getSortValues()[0]);
                assertEquals("1.3.567#12", hits[7].getSortValues()[0]);
            }
        );
    }

    public void testRegexQuery() throws Exception {
        String indexName = "test_regex";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).build(), "_doc", "version", "type=version");
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1")
            .setSource(jsonBuilder().startObject().field("version", "1.0.0alpha2.1.0-rc.1").endObject())
            .get();
        prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().field("version", "1.3.0+build.1234567").endObject()).get();
        prepareIndex(indexName).setId("3").setSource(jsonBuilder().startObject().field("version", "2.1.0-alpha.beta").endObject()).get();
        prepareIndex(indexName).setId("4").setSource(jsonBuilder().startObject().field("version", "2.1.0").endObject()).get();
        prepareIndex(indexName).setId("5").setSource(jsonBuilder().startObject().field("version", "2.33.0").endObject()).get();
        client().admin().indices().prepareRefresh(indexName).get();

        assertResponse(client().prepareSearch(indexName).setQuery(QueryBuilders.regexpQuery("version", "2.*0")), response -> {
            assertEquals(2, response.getHits().getTotalHits().value());
            assertEquals("2.1.0", response.getHits().getHits()[0].getSourceAsMap().get("version"));
            assertEquals("2.33.0", response.getHits().getHits()[1].getSourceAsMap().get("version"));
        });

        assertResponse(
            client().prepareSearch(indexName).setQuery(QueryBuilders.regexpQuery("version", "<0-10>.<0-10>.*al.*")),
            response -> {
                assertEquals(2, response.getHits().getTotalHits().value());
                assertEquals("1.0.0alpha2.1.0-rc.1", response.getHits().getHits()[0].getSourceAsMap().get("version"));
                assertEquals("2.1.0-alpha.beta", response.getHits().getHits()[1].getSourceAsMap().get("version"));
            }
        );

        assertResponse(client().prepareSearch(indexName).setQuery(QueryBuilders.regexpQuery("version", "1.[0-9].[0-9].*")), response -> {
            assertEquals(2, response.getHits().getTotalHits().value());
            assertEquals("1.0.0alpha2.1.0-rc.1", response.getHits().getHits()[0].getSourceAsMap().get("version"));
            assertEquals("1.3.0+build.1234567", response.getHits().getHits()[1].getSourceAsMap().get("version"));
        });

        // test case sensitivity / insensitivity
        assertResponse(client().prepareSearch(indexName).setQuery(QueryBuilders.regexpQuery("version", ".*alpha.*")), response -> {
            assertEquals(2, response.getHits().getTotalHits().value());
            assertEquals("1.0.0alpha2.1.0-rc.1", response.getHits().getHits()[0].getSourceAsMap().get("version"));
            assertEquals("2.1.0-alpha.beta", response.getHits().getHits()[1].getSourceAsMap().get("version"));
        });

        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.regexpQuery("version", ".*Alpha.*")), 0);

        assertResponse(
            client().prepareSearch(indexName).setQuery(QueryBuilders.regexpQuery("version", ".*Alpha.*").caseInsensitive(true)),
            response -> {
                assertEquals(2, response.getHits().getTotalHits().value());
                assertEquals("1.0.0alpha2.1.0-rc.1", response.getHits().getHits()[0].getSourceAsMap().get("version"));
                assertEquals("2.1.0-alpha.beta", response.getHits().getHits()[1].getSourceAsMap().get("version"));
            }
        );
    }

    public void testFuzzyQuery() throws Exception {
        String indexName = "test_fuzzy";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).build(), "_doc", "version", "type=version");
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1")
            .setSource(jsonBuilder().startObject().field("version", "1.0.0-alpha.2.1.0-rc.1").endObject())
            .get();
        prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().field("version", "1.3.0+build.1234567").endObject()).get();
        prepareIndex(indexName).setId("3").setSource(jsonBuilder().startObject().field("version", "2.1.0-alpha.beta").endObject()).get();
        prepareIndex(indexName).setId("4").setSource(jsonBuilder().startObject().field("version", "2.1.0").endObject()).get();
        prepareIndex(indexName).setId("5").setSource(jsonBuilder().startObject().field("version", "2.33.0").endObject()).get();
        prepareIndex(indexName).setId("6").setSource(jsonBuilder().startObject().field("version", "2.a3.0").endObject()).get();
        client().admin().indices().prepareRefresh(indexName).get();

        assertResponse(client().prepareSearch(indexName).setQuery(QueryBuilders.fuzzyQuery("version", "2.3.0")), response -> {
            assertEquals(3, response.getHits().getTotalHits().value());
            assertEquals("2.1.0", response.getHits().getHits()[0].getSourceAsMap().get("version"));
            assertEquals("2.33.0", response.getHits().getHits()[1].getSourceAsMap().get("version"));
            assertEquals("2.a3.0", response.getHits().getHits()[2].getSourceAsMap().get("version"));
        });
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

        for (String version : List.of(
            "1.0.0-alpha.2.1.0-rc.1",
            "1.3.0+build.1234567",
            "2.1.0-alpha.beta",
            "2.1.0",
            "2.33.0",
            "3.1.1-a",
            "3.1.1+b",
            "3.1.123"
        )) {
            prepareIndex(indexName).setSource(jsonBuilder().startObject().field("version", version).endObject()).get();
        }
        client().admin().indices().prepareRefresh(indexName).get();

        checkWildcardQuery(indexName, "*alpha*", new String[] { "1.0.0-alpha.2.1.0-rc.1", "2.1.0-alpha.beta" });
        checkWildcardQuery(indexName, "*b*", new String[] { "1.3.0+build.1234567", "2.1.0-alpha.beta", "3.1.1+b" });
        checkWildcardQuery(indexName, "*bet*", new String[] { "2.1.0-alpha.beta" });
        checkWildcardQuery(indexName, "2.1*", new String[] { "2.1.0-alpha.beta", "2.1.0" });
        checkWildcardQuery(indexName, "2.1.0-*", new String[] { "2.1.0-alpha.beta" });
        checkWildcardQuery(indexName, "*2.1.0-*", new String[] { "1.0.0-alpha.2.1.0-rc.1", "2.1.0-alpha.beta" });
        checkWildcardQuery(indexName, "*2.1.0*", new String[] { "1.0.0-alpha.2.1.0-rc.1", "2.1.0-alpha.beta", "2.1.0" });
        checkWildcardQuery(indexName, "*2.?.0*", new String[] { "1.0.0-alpha.2.1.0-rc.1", "2.1.0-alpha.beta", "2.1.0" });
        checkWildcardQuery(indexName, "*2.??.0*", new String[] { "2.33.0" });
        checkWildcardQuery(indexName, "?.1.0", new String[] { "2.1.0" });
        checkWildcardQuery(indexName, "*-*", new String[] { "1.0.0-alpha.2.1.0-rc.1", "2.1.0-alpha.beta", "3.1.1-a" });
        checkWildcardQuery(indexName, "1.3.0+b*", new String[] { "1.3.0+build.1234567" });
        checkWildcardQuery(indexName, "3.1.1??", new String[] { "3.1.1-a", "3.1.1+b", "3.1.123" });

        // test case sensitivity / insensitivity
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", "*Alpha*")), 0);

        assertResponse(
            client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", "*Alpha*").caseInsensitive(true)),
            response -> {
                assertEquals(2, response.getHits().getTotalHits().value());
                assertEquals("1.0.0-alpha.2.1.0-rc.1", response.getHits().getHits()[0].getSourceAsMap().get("version"));
                assertEquals("2.1.0-alpha.beta", response.getHits().getHits()[1].getSourceAsMap().get("version"));
            }
        );
    }

    private void checkWildcardQuery(String indexName, String query, String... expectedResults) {
        assertResponse(client().prepareSearch(indexName).setQuery(QueryBuilders.wildcardQuery("version", query)), response -> {
            assertEquals(expectedResults.length, response.getHits().getTotalHits().value());
            for (int i = 0; i < expectedResults.length; i++) {
                String expected = expectedResults[i];
                Object actual = response.getHits().getHits()[i].getSourceAsMap().get("version");
                assertEquals("expected " + expected + " in position " + i + " but found " + actual, expected, actual);
            }
        });
    }

    /**
     * test that versions that are invalid under semver are still indexed and retrieveable, though they sort differently
     */
    public void testStoreMalformed() throws Exception {
        String indexName = "test_malformed";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).build(), "_doc", "version", "type=version");
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1").setSource(jsonBuilder().startObject().field("version", "1.invalid.0").endObject()).get();
        prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().field("version", "2.2.0").endObject()).get();
        prepareIndex(indexName).setId("3").setSource(jsonBuilder().startObject().field("version", "2.2.0-badchar!").endObject()).get();
        prepareIndex(indexName).setId("4").setSource(jsonBuilder().startObject().field("version", "").endObject()).get();
        client().admin().indices().prepareRefresh(indexName).get();

        assertResponse(client().prepareSearch(indexName).addDocValueField("version"), response -> {
            assertEquals(4, response.getHits().getTotalHits().value());
            assertEquals("1", response.getHits().getAt(0).getId());
            assertEquals("1.invalid.0", response.getHits().getAt(0).field("version").getValue());

            assertEquals("2", response.getHits().getAt(1).getId());
            assertEquals("2.2.0", response.getHits().getAt(1).field("version").getValue());

            assertEquals("3", response.getHits().getAt(2).getId());
            assertEquals("2.2.0-badchar!", response.getHits().getAt(2).field("version").getValue());

            assertEquals("4", response.getHits().getAt(3).getId());
            assertEquals("", response.getHits().getAt(3).field("version").getValue());
        });

        // exact match for malformed term
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", "1.invalid.0")), 1);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", "2.2.0-badchar!")), 1);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", "")), 1);

        // also should appear in terms aggs
        assertResponse(
            client().prepareSearch(indexName).addAggregation(AggregationBuilders.terms("myterms").field("version")),
            response -> {
                Terms terms = response.getAggregations().get("myterms");
                List<? extends Bucket> buckets = terms.getBuckets();

                assertEquals(4, buckets.size());
                assertEquals("2.2.0", buckets.get(0).getKey());
                assertEquals("", buckets.get(1).getKey());
                assertEquals("1.invalid.0", buckets.get(2).getKey());
                assertEquals("2.2.0-badchar!", buckets.get(3).getKey());
            }
        );

        // invalid values should sort after all valid ones
        assertResponse(
            client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).addSort("version", SortOrder.ASC),
            response -> {
                assertEquals(4, response.getHits().getTotalHits().value());
                SearchHit[] hits = response.getHits().getHits();
                assertEquals("2.2.0", hits[0].getSortValues()[0]);
                assertEquals("", hits[1].getSortValues()[0]);
                assertEquals("1.invalid.0", hits[2].getSortValues()[0]);
                assertEquals("2.2.0-badchar!", hits[3].getSortValues()[0]);
            }
        );

        // ranges can include them, but they are sorted last
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").to("3.0.0")), 1);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("3.0.0")), 3);

        // using the empty string as lower bound should return all "invalid" versions
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("")), 3);
    }

    public void testAggs() throws Exception {
        String indexName = "test_aggs";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).build(), "_doc", "version", "type=version");
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1").setSource(jsonBuilder().startObject().field("version", "1.0").endObject()).get();
        prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().field("version", "1.3.0").endObject()).get();
        prepareIndex(indexName).setId("3").setSource(jsonBuilder().startObject().field("version", "2.1.0-alpha").endObject()).get();
        prepareIndex(indexName).setId("4").setSource(jsonBuilder().startObject().field("version", "2.1.0").endObject()).get();
        prepareIndex(indexName).setId("5").setSource(jsonBuilder().startObject().field("version", "3.11.5").endObject()).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // terms aggs
        assertResponse(
            client().prepareSearch(indexName).addAggregation(AggregationBuilders.terms("myterms").field("version")),
            response -> {
                Terms terms = response.getAggregations().get("myterms");
                List<? extends Bucket> buckets = terms.getBuckets();

                assertEquals(5, buckets.size());
                assertEquals("1.0", buckets.get(0).getKey());
                assertEquals("1.3.0", buckets.get(1).getKey());
                assertEquals("2.1.0-alpha", buckets.get(2).getKey());
                assertEquals("2.1.0", buckets.get(3).getKey());
                assertEquals("3.11.5", buckets.get(4).getKey());
            }
        );

        // cardinality
        assertResponse(
            client().prepareSearch(indexName).addAggregation(AggregationBuilders.cardinality("myterms").field("version")),
            response -> {
                Cardinality card = response.getAggregations().get("myterms");
                assertEquals(5, card.getValue());
            }
        );

        // string stats
        assertResponse(
            client().prepareSearch(indexName).addAggregation(new StringStatsAggregationBuilder("stats").field("version")),
            response -> {
                InternalStringStats stats = response.getAggregations().get("stats");
                assertEquals(3, stats.getMinLength());
                assertEquals(11, stats.getMaxLength());
            }
        );
    }

    public void testMultiValues() throws Exception {
        String indexName = "test_multi";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).build(), "_doc", "version", "type=version");
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1").setSource(jsonBuilder().startObject().array("version", "1.0.0", "3.0.0").endObject()).get();
        prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().array("version", "2.0.0", "4.alpha.0").endObject()).get();
        prepareIndex(indexName).setId("3")
            .setSource(jsonBuilder().startObject().array("version", "2.1.0", "2.2.0", "5.99.0").endObject())
            .get();
        client().admin().indices().prepareRefresh(indexName).get();

        assertResponse(client().prepareSearch(indexName).addSort("version", SortOrder.ASC), response -> {
            assertEquals(3, response.getHits().getTotalHits().value());
            assertEquals("1", response.getHits().getAt(0).getId());
            assertEquals("2", response.getHits().getAt(1).getId());
            assertEquals("3", response.getHits().getAt(2).getId());
        });

        assertResponse(client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", "3.0.0")), response -> {
            assertEquals(1, response.getHits().getTotalHits().value());
            assertEquals("1", response.getHits().getAt(0).getId());
        });

        assertResponse(client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("version", "4.alpha.0")), response -> {
            assertEquals(1, response.getHits().getTotalHits().value());
            assertEquals("2", response.getHits().getAt(0).getId());
        });

        // range
        assertResponse(
            client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").to("1.5.0")),
            response -> assertEquals(1, response.getHits().getTotalHits().value())
        );

        assertResponse(
            client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("1.5.0")),
            response -> assertEquals(3, response.getHits().getTotalHits().value())
        );

        assertResponse(
            client().prepareSearch(indexName).setQuery(QueryBuilders.rangeQuery("version").from("5.0.0").to("6.0.0")),
            response -> assertEquals(1, response.getHits().getTotalHits().value())
        );
    }
}
