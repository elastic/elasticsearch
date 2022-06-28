/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.Version;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Map;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.search.AbstractSearchTestCase;
import org.elasticsearch.search.SearchSortValuesAndFormatsTests;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
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

    public void testAllowPartialResultsSerializationPre7_0_0() throws IOException {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, VersionUtils.getPreviousVersion(Version.V_7_0_0));
        ShardSearchRequest shardSearchTransportRequest = createShardSearchRequest();
        if (shardSearchTransportRequest.source() != null) {
            shardSearchTransportRequest.source().runtimeMappings(emptyMap());
        }
        ShardSearchRequest deserializedRequest = copyWriteable(
            shardSearchTransportRequest,
            namedWriteableRegistry,
            ShardSearchRequest::new,
            version
        );
        if (version.before(Version.V_6_3_0)) {
            assertFalse(deserializedRequest.allowPartialSearchResults());
        } else {
            assertEquals(shardSearchTransportRequest.allowPartialSearchResults(), deserializedRequest.allowPartialSearchResults());
        }
    }

    private ShardSearchRequest createShardSearchRequest() throws IOException {
        SearchRequest searchRequest = createSearchRequest();
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(2, 10), randomAlphaOfLengthBetween(2, 10), randomInt());
        final AliasFilter filteringAliases;
        if (randomBoolean()) {
            String[] strings = generateRandomStringArray(10, 10, false, false);
            filteringAliases = new AliasFilter(RandomQueryBuilder.createQuery(random()), strings);
        } else {
            filteringAliases = new AliasFilter(null, Strings.EMPTY_ARRAY);
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
                return parseInnerQueryBuilder(parser);
            }
        }, indexMetadata, aliasNames);
    }

    ShardSearchRequest serialize(ShardSearchRequest request, Version version) throws IOException {
        if (version.before(Version.V_7_11_0)) {
            if (request.source() != null) {
                request.source().runtimeMappings(Map.of());
            }
        }
        return copyWriteable(request, namedWriteableRegistry, ShardSearchRequest::new, version);
    }

    public void testChannelVersion() throws Exception {
        ShardSearchRequest request = createShardSearchRequest();
        Version channelVersion = Version.CURRENT;
        assertThat(request.getChannelVersion(), equalTo(channelVersion));
        int iterations = between(0, 5);
        // New version
        for (int i = 0; i < iterations; i++) {
            Version newVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_12_1, Version.CURRENT);
            request = serialize(request, newVersion);
            channelVersion = Version.min(newVersion, channelVersion);
            assertThat(request.getChannelVersion(), equalTo(channelVersion));
            if (randomBoolean()) {
                request = new ShardSearchRequest(request);
            }
        }
        // Old version
        iterations = between(1, 5);
        for (int i = 0; i < iterations; i++) {
            channelVersion = VersionUtils.randomVersionBetween(
                random(),
                Version.V_7_0_0,
                VersionUtils.getPreviousVersion(Version.V_7_12_1)
            );
            request = serialize(request, channelVersion);
            assertThat(request.getChannelVersion(), equalTo(channelVersion));
            if (randomBoolean()) {
                request = new ShardSearchRequest(request);
            }
        }
        // Any version
        iterations = between(1, 5);
        for (int i = 0; i < iterations; i++) {
            Version version = VersionUtils.randomVersion(random());
            request = serialize(request, version);
            if (version.onOrAfter(Version.V_7_12_1)) {
                channelVersion = Version.min(channelVersion, version);
            } else {
                channelVersion = version;
            }
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
}
