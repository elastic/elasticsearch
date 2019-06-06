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

package org.elasticsearch.search.internal;

import org.elasticsearch.Version;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.search.AbstractSearchTestCase;

import java.io.IOException;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ShardSearchTransportRequestTests extends AbstractSearchTestCase {
    private IndexMetaData baseMetaData = IndexMetaData.builder("test").settings(Settings.builder()
        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build())
        .numberOfShards(1).numberOfReplicas(1).build();

    public void testSerialization() throws Exception {
        ShardSearchTransportRequest shardSearchTransportRequest = createShardSearchTransportRequest();
        ShardSearchTransportRequest deserializedRequest =
            copyWriteable(shardSearchTransportRequest, namedWriteableRegistry, ShardSearchTransportRequest::new);
        assertEquals(deserializedRequest.scroll(), shardSearchTransportRequest.scroll());
        assertEquals(deserializedRequest.getAliasFilter(), shardSearchTransportRequest.getAliasFilter());
        assertArrayEquals(deserializedRequest.indices(), shardSearchTransportRequest.indices());
        assertEquals(deserializedRequest.indicesOptions(), shardSearchTransportRequest.indicesOptions());
        assertEquals(deserializedRequest.nowInMillis(), shardSearchTransportRequest.nowInMillis());
        assertEquals(deserializedRequest.source(), shardSearchTransportRequest.source());
        assertEquals(deserializedRequest.searchType(), shardSearchTransportRequest.searchType());
        assertEquals(deserializedRequest.shardId(), shardSearchTransportRequest.shardId());
        assertEquals(deserializedRequest.numberOfShards(), shardSearchTransportRequest.numberOfShards());
        assertArrayEquals(deserializedRequest.indexRoutings(), shardSearchTransportRequest.indexRoutings());
        assertEquals(deserializedRequest.preference(), shardSearchTransportRequest.preference());
        assertEquals(deserializedRequest.cacheKey(), shardSearchTransportRequest.cacheKey());
        assertNotSame(deserializedRequest, shardSearchTransportRequest);
        assertEquals(deserializedRequest.getAliasFilter(), shardSearchTransportRequest.getAliasFilter());
        assertEquals(deserializedRequest.indexBoost(), shardSearchTransportRequest.indexBoost(), 0.0f);
        assertEquals(deserializedRequest.getClusterAlias(), shardSearchTransportRequest.getClusterAlias());
        assertEquals(shardSearchTransportRequest.allowPartialSearchResults(), deserializedRequest.allowPartialSearchResults());
    }

    private ShardSearchTransportRequest createShardSearchTransportRequest() throws IOException {
        SearchRequest searchRequest = createSearchRequest();
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(2, 10), randomAlphaOfLengthBetween(2, 10), randomInt());
        final AliasFilter filteringAliases;
        if (randomBoolean()) {
            String[] strings = generateRandomStringArray(10, 10, false, false);
            filteringAliases = new AliasFilter(RandomQueryBuilder.createQuery(random()), strings);
        } else {
            filteringAliases = new AliasFilter(null, Strings.EMPTY_ARRAY);
        }
        final String[] routings = generateRandomStringArray(5, 10, false, true);
        return new ShardSearchTransportRequest(new OriginalIndices(searchRequest), searchRequest, shardId,
            randomIntBetween(1, 100), filteringAliases, randomBoolean() ? 1.0f : randomFloat(),
            Math.abs(randomLong()), randomAlphaOfLengthBetween(3, 10), routings);
    }

    public void testFilteringAliases() throws Exception {
        IndexMetaData indexMetaData = baseMetaData;
        indexMetaData = add(indexMetaData, "cats", filter(termQuery("animal", "cat")));
        indexMetaData = add(indexMetaData, "dogs", filter(termQuery("animal", "dog")));
        indexMetaData = add(indexMetaData, "all", null);

        assertThat(indexMetaData.getAliases().containsKey("cats"), equalTo(true));
        assertThat(indexMetaData.getAliases().containsKey("dogs"), equalTo(true));
        assertThat(indexMetaData.getAliases().containsKey("turtles"), equalTo(false));

        assertEquals(aliasFilter(indexMetaData, "cats"), QueryBuilders.termQuery("animal", "cat"));
        assertEquals(aliasFilter(indexMetaData, "cats", "dogs"), QueryBuilders.boolQuery().should(QueryBuilders.termQuery("animal", "cat"))
            .should(QueryBuilders.termQuery("animal", "dog")));

        // Non-filtering alias should turn off all filters because filters are ORed
        assertThat(aliasFilter(indexMetaData,"all"), nullValue());
        assertThat(aliasFilter(indexMetaData, "cats", "all"), nullValue());
        assertThat(aliasFilter(indexMetaData, "all", "cats"), nullValue());

        indexMetaData = add(indexMetaData, "cats", filter(termQuery("animal", "feline")));
        indexMetaData = add(indexMetaData, "dogs", filter(termQuery("animal", "canine")));
        assertEquals(aliasFilter(indexMetaData, "dogs", "cats"),QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("animal", "canine"))
            .should(QueryBuilders.termQuery("animal", "feline")));
    }

    public void testRemovedAliasFilter() throws Exception {
        IndexMetaData indexMetaData = baseMetaData;
        indexMetaData = add(indexMetaData, "cats", filter(termQuery("animal", "cat")));
        indexMetaData = remove(indexMetaData, "cats");
        try {
            aliasFilter(indexMetaData, "cats");
            fail("Expected InvalidAliasNameException");
        } catch (InvalidAliasNameException e) {
            assertThat(e.getMessage(), containsString("Invalid alias name [cats]"));
        }
    }

    public void testUnknownAliasFilter() throws Exception {
        IndexMetaData indexMetaData = baseMetaData;
        indexMetaData = add(indexMetaData, "cats", filter(termQuery("animal", "cat")));
        indexMetaData = add(indexMetaData, "dogs", filter(termQuery("animal", "dog")));
        IndexMetaData finalIndexMetadata = indexMetaData;
        expectThrows(InvalidAliasNameException.class, () -> aliasFilter(finalIndexMetadata, "unknown"));
    }

    public static CompressedXContent filter(QueryBuilder filterBuilder) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        filterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.close();
        return new CompressedXContent(Strings.toString(builder));
    }

    private IndexMetaData remove(IndexMetaData indexMetaData, String alias) {
        return IndexMetaData.builder(indexMetaData).removeAlias(alias).build();
    }

    private IndexMetaData add(IndexMetaData indexMetaData, String alias, @Nullable CompressedXContent filter) {
        return IndexMetaData.builder(indexMetaData).putAlias(AliasMetaData.builder(alias).filter(filter).build()).build();
    }

    public QueryBuilder aliasFilter(IndexMetaData indexMetaData, String... aliasNames) {
        CheckedFunction<byte[], QueryBuilder, IOException> filterParser = bytes -> {
            try (XContentParser parser = XContentFactory.xContent(bytes)
                    .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes)) {
                return parseInnerQueryBuilder(parser);
            }
        };
        return ShardSearchRequest.parseAliasFilter(filterParser, indexMetaData, aliasNames);
    }
}
