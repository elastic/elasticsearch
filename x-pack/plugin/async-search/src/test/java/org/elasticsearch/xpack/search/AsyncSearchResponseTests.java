/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.PartialSearchResponse;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformNamedXContentProvider;
import org.elasticsearch.xpack.core.transform.transforms.SyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.search.GetAsyncSearchRequestTests.randomSearchId;

public class AsyncSearchResponseTests extends ESTestCase {
    private SearchResponse searchResponse = randomSearchResponse();
    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteables.add(new NamedWriteableRegistry.Entry(SyncConfig.class, TransformField.TIME_BASED_SYNC.getPreferredName(),
            TimeSyncConfig::new));

        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.addAll(new TransformNamedXContentProvider().getNamedXContentParsers());

        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }


    protected Writeable.Reader<AsyncSearchResponse> instanceReader() {
        return AsyncSearchResponse::new;
    }

    protected AsyncSearchResponse createTestInstance() {
        return randomAsyncSearchResponse(randomSearchId(), searchResponse);
    }

    protected void assertEqualInstances(AsyncSearchResponse expectedInstance, AsyncSearchResponse newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertEqualResponses(expectedInstance, newInstance);
    }

    public final void testSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            AsyncSearchResponse testInstance = createTestInstance();
            assertSerialization(testInstance);
        }
    }

    protected final AsyncSearchResponse assertSerialization(AsyncSearchResponse testInstance) throws IOException {
        return assertSerialization(testInstance, Version.CURRENT);
    }

    protected final AsyncSearchResponse assertSerialization(AsyncSearchResponse testInstance, Version version) throws IOException {
        AsyncSearchResponse deserializedInstance = copyInstance(testInstance, version);
        assertEqualInstances(testInstance, deserializedInstance);
        return deserializedInstance;
    }

    protected final AsyncSearchResponse copyInstance(AsyncSearchResponse instance) throws IOException {
        return copyInstance(instance, Version.CURRENT);
    }

    protected AsyncSearchResponse copyInstance(AsyncSearchResponse instance, Version version) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, instanceReader(), version);
    }

    static AsyncSearchResponse randomAsyncSearchResponse(String searchId, SearchResponse searchResponse) {
        int rand = randomIntBetween(0, 3);
        switch (rand) {
            case 0:
                return new AsyncSearchResponse(searchId, randomIntBetween(0, Integer.MAX_VALUE), randomBoolean());

            case 1:
                return new AsyncSearchResponse(searchId, searchResponse, randomIntBetween(0, Integer.MAX_VALUE), randomBoolean());

            case 2:
                return new AsyncSearchResponse(searchId, randomPartialSearchResponse(),
                    randomIntBetween(0, Integer.MAX_VALUE), randomBoolean());

            case 3:
                return new AsyncSearchResponse(searchId, randomPartialSearchResponse(),
                    new ElasticsearchException(new IOException("boum")), randomIntBetween(0, Integer.MAX_VALUE), randomBoolean());

            default:
                throw new AssertionError();
        }
    }

    static SearchResponse randomSearchResponse() {
        long tookInMillis = randomNonNegativeLong();
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = totalShards - successfulShards;
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        return new SearchResponse(internalSearchResponse, null, totalShards,
            successfulShards, skippedShards, tookInMillis, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    private static PartialSearchResponse randomPartialSearchResponse() {
        int totalShards = randomIntBetween(0, 10000);
        if (randomBoolean()) {
            return new PartialSearchResponse(totalShards);
        } else {
            int successfulShards = randomIntBetween(0, totalShards);
            int failedShards = totalShards - successfulShards;
            TotalHits totalHits = new TotalHits(randomLongBetween(0, Long.MAX_VALUE), randomFrom(TotalHits.Relation.values()));
            InternalMax max = new InternalMax("max", 0f, DocValueFormat.RAW, Collections.emptyList(), Collections.emptyMap());
            InternalAggregations aggs = new InternalAggregations(Collections.singletonList(max));
            return new PartialSearchResponse(totalShards, successfulShards, failedShards, totalHits, aggs);
        }
    }

    static void assertEqualResponses(AsyncSearchResponse expected, AsyncSearchResponse actual) {
        assertEquals(expected.id(), actual.id());
        assertEquals(expected.getVersion(), actual.getVersion());
        assertEquals(expected.status(), actual.status());
        assertEquals(expected.getPartialResponse(), actual.getPartialResponse());
        assertEquals(expected.getFailure() == null, actual.getFailure() == null);
        // TODO check equals response
    }
}
