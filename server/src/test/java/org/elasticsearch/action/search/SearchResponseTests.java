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

package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchHitsTests;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationsTests;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.profile.SearchProfileShardResultsTests;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
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
            new SearchModule(Settings.EMPTY, false, emptyList()).getNamedWriteables());
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
            SearchHits hits = SearchHitsTests.createTestItem();
            InternalAggregations aggregations = aggregationsTests.createTestInstance();
            Suggest suggest = SuggestTests.createTestItem();
            SearchProfileShardResults profileShardResults = SearchProfileShardResultsTests.createTestItem();
            internalSearchResponse = new InternalSearchResponse(hits, aggregations, suggest, profileShardResults,
                timedOut, terminatedEarly, numReducePhases);
        } else {
            internalSearchResponse = InternalSearchResponse.empty();
        }

        return new SearchResponse(internalSearchResponse, null, totalShards, successfulShards, skippedShards, tookInMillis,
            shardSearchFailures, randomBoolean() ? randomClusters() : SearchResponse.Clusters.EMPTY);
    }

    private static SearchResponse.Clusters randomClusters() {
        int totalClusters = randomIntBetween(0, 10);
        int successfulClusters = randomIntBetween(0, totalClusters);
        int skippedClusters = totalClusters - successfulClusters;
        return new SearchResponse.Clusters(totalClusters, successfulClusters, skippedClusters);
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
        BytesReference originalBytes = toShuffledXContent(response, xcontentType, params, humanReadable);
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
            failures[i] = ShardSearchFailureTests.createTestItem();
        }
        SearchResponse response = createTestItem(failures);
        XContentType xcontentType = randomFrom(XContentType.values());
        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        BytesReference originalBytes = toShuffledXContent(response, xcontentType, params, randomBoolean());
        try (XContentParser parser = createParser(xcontentType.xContent(), originalBytes)) {
            SearchResponse parsed = SearchResponse.fromXContent(parser);
            for (int i = 0; i < parsed.getShardFailures().length; i++) {
                ShardSearchFailure parsedFailure = parsed.getShardFailures()[i];
                ShardSearchFailure originalFailure = failures[i];
                assertEquals(originalFailure.index(), parsedFailure.index());
                assertEquals(originalFailure.shard(), parsedFailure.shard());
                assertEquals(originalFailure.shardId(), parsedFailure.shardId());
                String originalMsg = originalFailure.getCause().getMessage();
                assertEquals(parsedFailure.getCause().getMessage(), "Elasticsearch exception [type=parsing_exception, reason=" +
                        originalMsg + "]");
                String nestedMsg = originalFailure.getCause().getCause().getMessage();
                assertEquals(parsedFailure.getCause().getCause().getMessage(),
                        "Elasticsearch exception [type=illegal_argument_exception, reason=" + nestedMsg + "]");
            }
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
    }

    public void testToXContent() {
        SearchHit hit = new SearchHit(1, "id1", new Text("type"), Collections.emptyMap());
        hit.score(2.0f);
        SearchHit[] hits = new SearchHit[] { hit };
        {
            SearchResponse response = new SearchResponse(
                    new InternalSearchResponse(new SearchHits(hits, 100, 1.5f), null, null, null, false, null, 1), null, 0, 0, 0, 0,
                    ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
            StringBuilder expectedString = new StringBuilder();
            expectedString.append("{");
            {
                expectedString.append("\"took\":0,");
                expectedString.append("\"timed_out\":false,");
                expectedString.append("\"_shards\":");
                {
                    expectedString.append("{\"total\":0,");
                    expectedString.append("\"successful\":0,");
                    expectedString.append("\"skipped\":0,");
                    expectedString.append("\"failed\":0},");
                }
                expectedString.append("\"hits\":");
                {
                    expectedString.append("{\"total\":100,");
                    expectedString.append("\"max_score\":1.5,");
                    expectedString.append("\"hits\":[{\"_type\":\"type\",\"_id\":\"id1\",\"_score\":2.0}]}");
                }
            }
            expectedString.append("}");
            assertEquals(expectedString.toString(), Strings.toString(response));
        }
        {
            SearchResponse response = new SearchResponse(
                    new InternalSearchResponse(new SearchHits(hits, 100, 1.5f), null, null, null, false, null, 1), null, 0, 0, 0, 0,
                    ShardSearchFailure.EMPTY_ARRAY, new SearchResponse.Clusters(5, 3, 2));
            StringBuilder expectedString = new StringBuilder();
            expectedString.append("{");
            {
                expectedString.append("\"took\":0,");
                expectedString.append("\"timed_out\":false,");
                expectedString.append("\"_shards\":");
                {
                    expectedString.append("{\"total\":0,");
                    expectedString.append("\"successful\":0,");
                    expectedString.append("\"skipped\":0,");
                    expectedString.append("\"failed\":0},");
                }
                expectedString.append("\"_clusters\":");
                {
                    expectedString.append("{\"total\":5,");
                    expectedString.append("\"successful\":3,");
                    expectedString.append("\"skipped\":2},");
                }
                expectedString.append("\"hits\":");
                {
                    expectedString.append("{\"total\":100,");
                    expectedString.append("\"max_score\":1.5,");
                    expectedString.append("\"hits\":[{\"_type\":\"type\",\"_id\":\"id1\",\"_score\":2.0}]}");
                }
            }
            expectedString.append("}");
            assertEquals(expectedString.toString(), Strings.toString(response));
        }
    }

    public void testSerialization() throws IOException {
        SearchResponse searchResponse = createTestItem(false);
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        searchResponse.writeTo(bytesStreamOutput);
        try (StreamInput in = new NamedWriteableAwareStreamInput(
                StreamInput.wrap(bytesStreamOutput.bytes().toBytesRef().bytes), namedWriteableRegistry)) {
            SearchResponse serialized = new SearchResponse();
            serialized.readFrom(in);
            assertEquals(searchResponse.getHits().totalHits, serialized.getHits().totalHits);
            assertEquals(searchResponse.getHits().getHits().length, serialized.getHits().getHits().length);
            assertEquals(searchResponse.getNumReducePhases(), serialized.getNumReducePhases());
            assertEquals(searchResponse.getFailedShards(), serialized.getFailedShards());
            assertEquals(searchResponse.getTotalShards(), serialized.getTotalShards());
            assertEquals(searchResponse.getSkippedShards(), serialized.getSkippedShards());
            assertEquals(searchResponse.getClusters(), serialized.getClusters());
        }
    }

    public void testSerializationBwc() throws IOException {
        final byte[] data = Base64.getDecoder().decode("AAAAAAAAAAAAAgABBQUAAAoAAAAAAAAA");
        final Version version = VersionUtils.randomVersionBetween(random(), Version.V_5_6_5, Version.V_6_0_0);
        try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(data), namedWriteableRegistry)) {
            in.setVersion(version);
            SearchResponse deserialized = new SearchResponse();
            deserialized.readFrom(in);
            assertSame(SearchResponse.Clusters.EMPTY, deserialized.getClusters());

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                deserialized.writeTo(out);
                try (StreamInput in2 = new NamedWriteableAwareStreamInput(StreamInput.wrap(out.bytes().toBytesRef().bytes),
                        namedWriteableRegistry)) {
                    in2.setVersion(version);
                    SearchResponse deserialized2 = new SearchResponse();
                    deserialized2.readFrom(in2);
                    assertSame(SearchResponse.Clusters.EMPTY, deserialized2.getClusters());
                }
            }
        }
    }
}
