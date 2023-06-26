/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.search.AbstractSearchTestCase;
import org.elasticsearch.search.SearchSortValuesAndFormatsTests;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseTopLevelQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ShardSearchRequestTests extends AbstractSearchTestCase {
    private static final IndexMetadata BASE_METADATA = IndexMetadata.builder("test")
        .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build())
        .numberOfShards(1)
        .numberOfReplicas(1)
        .build();

    public void testSerialization() throws Exception {
        ShardSearchRequest shardSearchTransportRequest = createShardSearchRequest();
        ShardSearchRequest deserializedRequest = copyWriteable(
            shardSearchTransportRequest,
            namedWriteableRegistry,
            ShardSearchRequest::new
        );
        assertEquals(shardSearchTransportRequest, deserializedRequest);
    }

    public void testClone() throws Exception {
        for (int i = 0; i < 10; i++) {
            ShardSearchRequest shardSearchTransportRequest = createShardSearchRequest();
            ShardSearchRequest clone = new ShardSearchRequest(shardSearchTransportRequest);
            assertEquals(shardSearchTransportRequest, clone);
        }
    }

    private ShardSearchRequest createShardSearchRequest() throws IOException {
        return createShardSearchReqest(createSearchRequest());
    }

    private ShardSearchRequest createShardSearchReqest(SearchRequest searchRequest) {
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(2, 10), randomAlphaOfLengthBetween(2, 10), randomInt());
        final AliasFilter filteringAliases;
        if (randomBoolean()) {
            String[] strings = generateRandomStringArray(10, 10, false, false);
            filteringAliases = AliasFilter.of(RandomQueryBuilder.createQuery(random()), strings);
        } else {
            filteringAliases = AliasFilter.EMPTY;
        }
        ShardSearchContextId shardSearchContextId = null;
        TimeValue keepAlive = null;
        if (randomBoolean()) {
            shardSearchContextId = new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong());
            if (randomBoolean()) {
                keepAlive = TimeValue.timeValueSeconds(randomIntBetween(0, 120));
            }
        }
        int numberOfShards = randomIntBetween(1, 100);
        ShardSearchRequest req = new ShardSearchRequest(
            new OriginalIndices(searchRequest),
            searchRequest,
            shardId,
            randomIntBetween(1, numberOfShards),
            numberOfShards,
            filteringAliases,
            randomBoolean() ? 1.0f : randomFloat(),
            Math.abs(randomLong()),
            randomAlphaOfLengthBetween(3, 10),
            shardSearchContextId,
            keepAlive
        );
        req.canReturnNullResponseIfMatchNoDocs(randomBoolean());
        if (randomBoolean()) {
            req.setBottomSortValues(SearchSortValuesAndFormatsTests.randomInstance());
        }
        return req;
    }

    public void testFilteringAliases() throws Exception {
        IndexMetadata indexMetadata = BASE_METADATA;
        indexMetadata = add(indexMetadata, "cats", filter(termQuery("animal", "cat")));
        indexMetadata = add(indexMetadata, "dogs", filter(termQuery("animal", "dog")));
        indexMetadata = add(indexMetadata, "all", null);

        assertThat(indexMetadata.getAliases().containsKey("cats"), equalTo(true));
        assertThat(indexMetadata.getAliases().containsKey("dogs"), equalTo(true));
        assertThat(indexMetadata.getAliases().containsKey("turtles"), equalTo(false));

        assertEquals(aliasFilter(indexMetadata, "cats"), QueryBuilders.termQuery("animal", "cat"));
        assertEquals(
            aliasFilter(indexMetadata, "cats", "dogs"),
            QueryBuilders.boolQuery().should(QueryBuilders.termQuery("animal", "cat")).should(QueryBuilders.termQuery("animal", "dog"))
        );

        // Non-filtering alias should turn off all filters because filters are ORed
        assertThat(aliasFilter(indexMetadata, "all"), nullValue());
        assertThat(aliasFilter(indexMetadata, "cats", "all"), nullValue());
        assertThat(aliasFilter(indexMetadata, "all", "cats"), nullValue());

        indexMetadata = add(indexMetadata, "cats", filter(termQuery("animal", "feline")));
        indexMetadata = add(indexMetadata, "dogs", filter(termQuery("animal", "canine")));
        assertEquals(
            aliasFilter(indexMetadata, "dogs", "cats"),
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("animal", "canine"))
                .should(QueryBuilders.termQuery("animal", "feline"))
        );
    }

    public void testRemovedAliasFilter() throws Exception {
        IndexMetadata indexMetadata = BASE_METADATA;
        indexMetadata = add(indexMetadata, "cats", filter(termQuery("animal", "cat")));
        indexMetadata = remove(indexMetadata, "cats");
        try {
            aliasFilter(indexMetadata, "cats");
            fail("Expected InvalidAliasNameException");
        } catch (InvalidAliasNameException e) {
            assertThat(e.getMessage(), containsString("Invalid alias name [cats]"));
        }
    }

    public void testUnknownAliasFilter() throws Exception {
        IndexMetadata indexMetadata = BASE_METADATA;
        indexMetadata = add(indexMetadata, "cats", filter(termQuery("animal", "cat")));
        indexMetadata = add(indexMetadata, "dogs", filter(termQuery("animal", "dog")));
        IndexMetadata finalIndexMetadata = indexMetadata;
        expectThrows(InvalidAliasNameException.class, () -> aliasFilter(finalIndexMetadata, "unknown"));
    }

    private static void assertEquals(ShardSearchRequest orig, ShardSearchRequest copy) throws IOException {
        assertEquals(orig.scroll(), copy.scroll());
        assertEquals(orig.getAliasFilter(), copy.getAliasFilter());
        assertArrayEquals(orig.indices(), copy.indices());
        assertEquals(orig.indicesOptions(), copy.indicesOptions());
        assertEquals(orig.nowInMillis(), copy.nowInMillis());
        assertEquals(orig.source(), copy.source());
        assertEquals(orig.searchType(), copy.searchType());
        assertEquals(orig.shardId(), copy.shardId());
        assertEquals(orig.numberOfShards(), copy.numberOfShards());
        assertEquals(orig.cacheKey(null), copy.cacheKey(null));
        assertNotSame(orig, copy);
        assertEquals(orig.getAliasFilter(), copy.getAliasFilter());
        assertEquals(orig.indexBoost(), copy.indexBoost(), 0.0f);
        assertEquals(orig.getClusterAlias(), copy.getClusterAlias());
        assertEquals(orig.allowPartialSearchResults(), copy.allowPartialSearchResults());
        assertEquals(orig.canReturnNullResponseIfMatchNoDocs(), orig.canReturnNullResponseIfMatchNoDocs());
    }

    public static CompressedXContent filter(QueryBuilder filterBuilder) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        filterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.close();
        return new CompressedXContent(Strings.toString(builder));
    }

    private IndexMetadata remove(IndexMetadata indexMetadata, String alias) {
        return IndexMetadata.builder(indexMetadata).removeAlias(alias).build();
    }

    private IndexMetadata add(IndexMetadata indexMetadata, String alias, @Nullable CompressedXContent filter) {
        return IndexMetadata.builder(indexMetadata).putAlias(AliasMetadata.builder(alias).filter(filter).build()).build();
    }

    public QueryBuilder aliasFilter(IndexMetadata indexMetadata, String... aliasNames) {
        return ShardSearchRequest.parseAliasFilter(bytes -> {
            try (
                InputStream inputStream = bytes.streamInput();
                XContentParser parser = XContentFactory.xContentType(inputStream)
                    .xContent()
                    .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, inputStream)
            ) {
                return parseTopLevelQuery(parser);
            }
        }, indexMetadata, aliasNames);
    }

    public void testChannelVersion() throws Exception {
        ShardSearchRequest request = createShardSearchRequest();
        TransportVersion channelVersion = TransportVersion.current();
        assertThat(request.getChannelVersion(), equalTo(channelVersion));
        int iterations = between(0, 5);
        // New version
        for (int i = 0; i < iterations; i++) {
            TransportVersion version = TransportVersionUtils.randomCompatibleVersion(random());
            if (request.isForceSyntheticSource()) {
                version = TransportVersionUtils.randomVersionBetween(random(), TransportVersion.V_8_4_0, TransportVersion.current());
            }
            if (Optional.ofNullable(request.source()).map(SearchSourceBuilder::knnSearch).map(List::size).orElse(0) > 1) {
                version = TransportVersionUtils.randomVersionBetween(random(), TransportVersion.V_8_7_0, TransportVersion.current());
            }
            if (request.source() != null && request.source().rankBuilder() != null) {
                version = TransportVersionUtils.randomVersionBetween(random(), TransportVersion.V_8_8_0, TransportVersion.current());
            }
            if (request.source() != null && request.source().subSearches().size() >= 2) {
                version = TransportVersionUtils.randomVersionBetween(random(), TransportVersion.V_8_500_013, TransportVersion.current());
            }
            request = copyWriteable(request, namedWriteableRegistry, ShardSearchRequest::new, version);
            channelVersion = TransportVersion.min(channelVersion, version);
            assertThat(request.getChannelVersion(), equalTo(channelVersion));
            if (randomBoolean()) {
                request = new ShardSearchRequest(request);
            }
        }
    }

    public void testWillCallRequestCacheKeyDifferentiators() throws IOException {
        final ShardSearchRequest shardSearchRequest = createShardSearchRequest();
        final AtomicBoolean invoked = new AtomicBoolean(false);
        final CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> differentiator = (r, o) -> {
            assertThat(r, sameInstance(shardSearchRequest));
            invoked.set(true);
        };
        shardSearchRequest.cacheKey(differentiator);
        assertThat(invoked.get(), is(true));
    }

    public void testForceSyntheticUnsupported() throws IOException {
        SearchRequest request = createSearchRequest();
        if (request.source() != null) {
            request.source().rankBuilder(null);
            if (request.source().subSearches().size() >= 2) {
                request.source().subSearches(new ArrayList<>());
            }
        }
        request.setForceSyntheticSource(true);
        ShardSearchRequest shardRequest = createShardSearchReqest(request);
        StreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.V_8_3_0);
        Exception e = expectThrows(IllegalArgumentException.class, () -> shardRequest.writeTo(out));
        assertEquals(e.getMessage(), "force_synthetic_source is not supported before 8.4.0");
    }
}
