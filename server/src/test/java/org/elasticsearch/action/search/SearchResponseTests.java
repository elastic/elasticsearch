/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchHitsTests;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationsTests;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileResultsTests;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class SearchResponseTests extends ESTestCase {

    private static final NamedXContentRegistry xContentRegistry;
    static {
        List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>(InternalAggregationTestCase.getDefaultNamedXContents());
        namedXContents.addAll(SuggestTests.getDefaultNamedXContents());
        xContentRegistry = new NamedXContentRegistry(namedXContents);
    }

    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, emptyList()).getNamedWriteables()
    );
    private AggregationsTests aggregationsTests = new AggregationsTests();

    @Before
    public void init() throws Exception {
        aggregationsTests.init();
    }

    @After
    public void cleanUp() throws Exception {
        aggregationsTests.cleanUp();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    private SearchResponse createTestItem(ShardSearchFailure... shardSearchFailures) {
        return createTestItem(false, shardSearchFailures);
    }

    /**
     * This SearchResponse doesn't include SearchHits, Aggregations, Suggestions, ShardSearchFailures, SearchProfileShardResults
     * to make it possible to only test properties of the SearchResponse itself
     */
    private SearchResponse createMinimalTestItem() {
        return createTestItem(true);
    }

    /**
     * if minimal is set, don't include search hits, aggregations, suggest etc... to make test simpler
     */
    private SearchResponse createTestItem(boolean minimal, ShardSearchFailure... shardSearchFailures) {
        boolean timedOut = randomBoolean();
        Boolean terminatedEarly = randomBoolean() ? null : randomBoolean();
        int numReducePhases = randomIntBetween(1, 10);
        long tookInMillis = randomNonNegativeLong();
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = randomIntBetween(0, totalShards);
        InternalSearchResponse internalSearchResponse;
        if (minimal == false) {
            SearchHits hits = SearchHitsTests.createTestItem(true, true);
            InternalAggregations aggregations = aggregationsTests.createTestInstance();
            Suggest suggest = SuggestTests.createTestItem();
            SearchProfileResults profileResults = SearchProfileResultsTests.createTestItem();
            internalSearchResponse = new InternalSearchResponse(
                hits,
                aggregations,
                suggest,
                profileResults,
                timedOut,
                terminatedEarly,
                numReducePhases
            );
        } else {
            internalSearchResponse = InternalSearchResponse.EMPTY_WITH_TOTAL_HITS;
        }

        return new SearchResponse(
            internalSearchResponse,
            null,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            shardSearchFailures,
            randomBoolean() ? randomClusters() : SearchResponse.Clusters.EMPTY
        );
    }

    static SearchResponse.Clusters randomClusters() {
        int totalClusters = randomIntBetween(0, 10);
        int successfulClusters = randomIntBetween(0, totalClusters);
        int skippedClusters = totalClusters - successfulClusters;
        if (randomBoolean()) {
            return new SearchResponse.Clusters(totalClusters, successfulClusters, skippedClusters);
        } else {
            int remoteClusters = totalClusters;
            if (totalClusters > 0 && randomBoolean()) {
                // remoteClusters can be same as total cluster count or one less (when doing local search)
                remoteClusters--;
            }
            // Clusters has an assert that if ccsMinimizeRoundtrips = true, then remoteClusters must be > 0
            boolean ccsMinimizeRoundtrips = (remoteClusters > 0 ? randomBoolean() : false);
            return new SearchResponse.Clusters(totalClusters, successfulClusters, skippedClusters, remoteClusters, ccsMinimizeRoundtrips);
        }
    }

    /**
     * the "_shard/total/failures" section makes it impossible to directly
     * compare xContent, so we omit it here
     */
    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(createTestItem(), false);
    }

    /**
     * This test adds random fields and objects to the xContent rendered out to
     * ensure we can parse it back to be forward compatible with additions to
     * the xContent. We test this with a "minimal" SearchResponse, adding random
     * fields to SearchHits, Aggregations etc... is tested in their own tests
     */
    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(createMinimalTestItem(), true);
    }

    private void doFromXContentTestWithRandomFields(SearchResponse response, boolean addRandomFields) throws IOException {
        XContentType xcontentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        BytesReference originalBytes = toShuffledXContent(
            ChunkedToXContent.wrapAsToXContent(response),
            xcontentType,
            params,
            humanReadable
        );
        BytesReference mutated;
        if (addRandomFields) {
            mutated = insertRandomFields(xcontentType, originalBytes, null, random());
        } else {
            mutated = originalBytes;
        }
        try (XContentParser parser = createParser(xcontentType.xContent(), mutated)) {
            SearchResponse parsed = SearchResponse.fromXContent(parser);
            assertToXContentEquivalent(originalBytes, XContentHelper.toXContent(parsed, xcontentType, params, humanReadable), xcontentType);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
    }

    /**
     * The "_shard/total/failures" section makes if impossible to directly compare xContent, because
     * the failures in the parsed SearchResponse are wrapped in an extra ElasticSearchException on the client side.
     * Because of this, in this special test case we compare the "top level" fields for equality
     * and the subsections xContent equivalence independently
     */
    public void testFromXContentWithFailures() throws IOException {
        int numFailures = randomIntBetween(1, 5);
        ShardSearchFailure[] failures = new ShardSearchFailure[numFailures];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = ShardSearchFailureTests.createTestItem(IndexMetadata.INDEX_UUID_NA_VALUE);
        }
        SearchResponse response = createTestItem(failures);
        XContentType xcontentType = randomFrom(XContentType.values());
        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        BytesReference originalBytes = toShuffledXContent(
            ChunkedToXContent.wrapAsToXContent(response),
            xcontentType,
            params,
            randomBoolean()
        );
        try (XContentParser parser = createParser(xcontentType.xContent(), originalBytes)) {
            SearchResponse parsed = SearchResponse.fromXContent(parser);
            for (int i = 0; i < parsed.getShardFailures().length; i++) {
                ShardSearchFailure parsedFailure = parsed.getShardFailures()[i];
                ShardSearchFailure originalFailure = failures[i];
                assertEquals(originalFailure.index(), parsedFailure.index());
                assertEquals(originalFailure.shard(), parsedFailure.shard());
                assertEquals(originalFailure.shardId(), parsedFailure.shardId());
                String originalMsg = originalFailure.getCause().getMessage();
                assertEquals(
                    parsedFailure.getCause().getMessage(),
                    "Elasticsearch exception [type=parsing_exception, reason=" + originalMsg + "]"
                );
                String nestedMsg = originalFailure.getCause().getCause().getMessage();
                assertEquals(
                    parsedFailure.getCause().getCause().getMessage(),
                    "Elasticsearch exception [type=illegal_argument_exception, reason=" + nestedMsg + "]"
                );
            }
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
    }

    public void testToXContent() throws IOException {
        SearchHit hit = new SearchHit(1, "id1");
        hit.score(2.0f);
        SearchHit[] hits = new SearchHit[] { hit };
        {
            SearchResponse response = new SearchResponse(
                new InternalSearchResponse(
                    new SearchHits(hits, new TotalHits(100, TotalHits.Relation.EQUAL_TO), 1.5f),
                    null,
                    null,
                    null,
                    false,
                    null,
                    1
                ),
                null,
                0,
                0,
                0,
                0,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
            String expectedString = XContentHelper.stripWhitespace("""
                {
                  "took": 0,
                  "timed_out": false,
                  "_shards": {
                    "total": 0,
                    "successful": 0,
                    "skipped": 0,
                    "failed": 0
                  },
                  "hits": {
                    "total": {
                      "value": 100,
                      "relation": "eq"
                    },
                    "max_score": 1.5,
                    "hits": [ { "_id": "id1", "_score": 2.0 } ]
                  }
                }""");
            assertEquals(expectedString, Strings.toString(response));
        }
        {
            SearchResponse response = new SearchResponse(
                new InternalSearchResponse(
                    new SearchHits(hits, new TotalHits(100, TotalHits.Relation.EQUAL_TO), 1.5f),
                    null,
                    null,
                    null,
                    false,
                    null,
                    1
                ),
                null,
                0,
                0,
                0,
                0,
                ShardSearchFailure.EMPTY_ARRAY,
                new SearchResponse.Clusters(5, 3, 2)
            );
            String expectedString = XContentHelper.stripWhitespace("""
                {
                  "took": 0,
                  "timed_out": false,
                  "_shards": {
                    "total": 0,
                    "successful": 0,
                    "skipped": 0,
                    "failed": 0
                  },
                  "_clusters": {
                    "total": 5,
                    "successful": 3,
                    "skipped": 2
                  },
                  "hits": {
                    "total": {
                      "value": 100,
                      "relation": "eq"
                    },
                    "max_score": 1.5,
                    "hits": [ { "_id": "id1", "_score": 2.0 } ]
                  }
                }""");
            assertEquals(expectedString, Strings.toString(response));
        }
    }

    public void testSerialization() throws IOException {
        SearchResponse searchResponse = createTestItem(false);
        SearchResponse deserialized = copyWriteable(searchResponse, namedWriteableRegistry, SearchResponse::new, TransportVersion.CURRENT);
        if (searchResponse.getHits().getTotalHits() == null) {
            assertNull(deserialized.getHits().getTotalHits());
        } else {
            assertEquals(searchResponse.getHits().getTotalHits().value, deserialized.getHits().getTotalHits().value);
            assertEquals(searchResponse.getHits().getTotalHits().relation, deserialized.getHits().getTotalHits().relation);
        }
        assertEquals(searchResponse.getHits().getHits().length, deserialized.getHits().getHits().length);
        assertEquals(searchResponse.getNumReducePhases(), deserialized.getNumReducePhases());
        assertEquals(searchResponse.getFailedShards(), deserialized.getFailedShards());
        assertEquals(searchResponse.getTotalShards(), deserialized.getTotalShards());
        assertEquals(searchResponse.getSkippedShards(), deserialized.getSkippedShards());
        assertEquals(searchResponse.getClusters(), deserialized.getClusters());
    }

    public void testToXContentEmptyClusters() throws IOException {
        SearchResponse searchResponse = new SearchResponse(
            InternalSearchResponse.EMPTY_WITH_TOTAL_HITS,
            null,
            1,
            1,
            0,
            1,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        SearchResponse deserialized = copyWriteable(searchResponse, namedWriteableRegistry, SearchResponse::new, TransportVersion.CURRENT);
        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        deserialized.getClusters().toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(0, Strings.toString(builder).length());
    }
}
