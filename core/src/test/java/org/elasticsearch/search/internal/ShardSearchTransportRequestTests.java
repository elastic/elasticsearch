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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.search.AbstractSearchTestCase;

import java.io.IOException;
import java.util.Base64;

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
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            shardSearchTransportRequest.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                ShardSearchTransportRequest deserializedRequest = new ShardSearchTransportRequest();
                deserializedRequest.readFrom(in);
                assertEquals(deserializedRequest.scroll(), shardSearchTransportRequest.scroll());
                assertEquals(deserializedRequest.filteringAliases(), shardSearchTransportRequest.filteringAliases());
                assertArrayEquals(deserializedRequest.indices(), shardSearchTransportRequest.indices());
                assertArrayEquals(deserializedRequest.types(), shardSearchTransportRequest.types());
                assertEquals(deserializedRequest.indicesOptions(), shardSearchTransportRequest.indicesOptions());
                assertEquals(deserializedRequest.isProfile(), shardSearchTransportRequest.isProfile());
                assertEquals(deserializedRequest.nowInMillis(), shardSearchTransportRequest.nowInMillis());
                assertEquals(deserializedRequest.source(), shardSearchTransportRequest.source());
                assertEquals(deserializedRequest.searchType(), shardSearchTransportRequest.searchType());
                assertEquals(deserializedRequest.shardId(), shardSearchTransportRequest.shardId());
                assertEquals(deserializedRequest.numberOfShards(), shardSearchTransportRequest.numberOfShards());
                assertEquals(deserializedRequest.cacheKey(), shardSearchTransportRequest.cacheKey());
                assertNotSame(deserializedRequest, shardSearchTransportRequest);
                assertEquals(deserializedRequest.filteringAliases(), shardSearchTransportRequest.filteringAliases());
                assertEquals(deserializedRequest.indexBoost(), shardSearchTransportRequest.indexBoost(), 0.0f);
            }
        }
    }

    private ShardSearchTransportRequest createShardSearchTransportRequest() throws IOException {
        SearchRequest searchRequest = createSearchRequest();
        ShardId shardId = new ShardId(randomAsciiOfLengthBetween(2, 10), randomAsciiOfLengthBetween(2, 10), randomInt());
        final AliasFilter filteringAliases;
        if (randomBoolean()) {
            String[] strings = generateRandomStringArray(10, 10, false, false);
            filteringAliases = new AliasFilter(RandomQueryBuilder.createQuery(random()), strings);
        } else {
            filteringAliases = new AliasFilter(null, Strings.EMPTY_ARRAY);
        }
        return new ShardSearchTransportRequest(searchRequest, shardId,
                randomIntBetween(1, 100), filteringAliases, randomBoolean() ? 1.0f : randomFloat(), Math.abs(randomLong()));
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
        return new CompressedXContent(builder.string());
    }

    private IndexMetaData remove(IndexMetaData indexMetaData, String alias) {
        IndexMetaData build = IndexMetaData.builder(indexMetaData).removeAlias(alias).build();
        return build;
    }

    private IndexMetaData add(IndexMetaData indexMetaData, String alias, @Nullable CompressedXContent filter) {
        return IndexMetaData.builder(indexMetaData).putAlias(AliasMetaData.builder(alias).filter(filter).build()).build();
    }

    public QueryBuilder aliasFilter(IndexMetaData indexMetaData, String... aliasNames) {
        CheckedFunction<byte[], QueryBuilder, IOException> filterParser = bytes -> {
            try (XContentParser parser = XContentFactory.xContent(bytes).createParser(xContentRegistry(), bytes)) {
                return new QueryParseContext(parser).parseInnerQueryBuilder();
            }
        };
        return ShardSearchRequest.parseAliasFilter(filterParser, indexMetaData, aliasNames);
    }

    // BWC test for changes from #20916
    public void testSerialize50Request() throws IOException {
        BytesArray requestBytes = new BytesArray(Base64.getDecoder()
            // this is a base64 encoded request generated with the same input
            .decode("AAh4cXptdEhJcgdnT0d1ZldWyfL/sgQBJAHkDAMBAAIBAQ4TWlljWlZ5TkVmRU5xQnFQVHBjVBRZbUpod2pRV2dDSXVxRXpRaEdGVBRFZWFJY0plT2hn" +
                "UEpISFhmSXR6Qw5XZ1hQcmFidWhWalFSQghuUWNwZ2JjQxBtZldRREJPaGF3UnlQSE56EVhQSUtRa25Iekh3bU5kbGVECWlFT2NIeEh3RgZIYXpMTWgUeGJq" +
                "VU9Tdkdua3RORU5QZkNrb1EOalRyWGh5WXhvZ3plV2UUcWlXZFl2eUFUSXdPVGdMUUtYTHAJU3RKR3JxQkVJEkdEQ01xUHpnWWNaT3N3U3prSRIUeURlVFpM" +
                "Q1lBZERZcWpDb3NOVWIST1NyQlZtdUNrd0F1UXRvdVRjEGp6RlVMd1dqc3VtUVNaTk0JT3N2cnpLQ3ZLBmRpS1J6cgdYbmVhZnBxBUlTUU9pEEJMcm1ERXVs" +
                "eXhESlBoVkgTaWdUUmtVZGh4d0FFc2ZKRm9ZahNrb01XTnFFd2NWSVVDU3pWS2xBC3JVTWV3V2tUUWJUE3VGQU1Hd21CYUFMTmNQZkxobXUIZ3dxWHBxWXcF" +
                "bmNDZUEOTFBSTEpYZVF6Z3d2eE0PV1BucUFacll6WWRxa1hCDGxkbXNMaVRzcUZXbAtSY0NsY3FNdlJQcv8BAP////8PAQAAARQAAQp5THlIcHdQeGtMAAAB" +
                "AQAAAAEDbkVLAQMBCgACAAADAQABAAAAAQhIc25wRGxQbwEBQgABAAACAQMAAAEIAAAJMF9OSG9kSmh2HwABAwljRW5MVWxFbVQFemlxWG8KcXZQTkRUUGJk" +
                "bgECCkpMbXVMT1dtVnkISEdUUHhsd0cBAAEJAAABA2lkcz+rKsUAAAAAAAAAAAECAQYAAgwxX0ZlRWxSQkhzQ07/////DwABAAEDCnRyYXFHR1hjVHkKTERY" +
                "aE1HRWVySghuSWtzbEtXUwABCgEHSlRwQnhwdwAAAQECAgAAAAAAAQcyX3FlYmNDGQEEBklxZU9iUQdTc01Gek5YCWlMd2xuamNRQwNiVncAAUHt61kAAQR0" +
                "ZXJtP4AAAAANbUtDSnpHU3lidm5KUBUMaVpqeG9vcm5QSFlvAAEBLGdtcWxuRWpWTXdvTlhMSHh0RWlFdHBnbEF1cUNmVmhoUVlwRFZxVllnWWV1A2ZvbwEA" +
                "AQhwYWlubGVzc/8AALk4AAAAAAABAAAAAAAAAwpKU09PU0ZmWnhFClVqTGxMa2p3V2gKdUJwZ3R3dXFER5Hg97uT7MOmPgEADw"));
        try (StreamInput in = new NamedWriteableAwareStreamInput(requestBytes.streamInput(), namedWriteableRegistry)) {
            in.setVersion(Version.V_5_0_0);
            ShardSearchTransportRequest readRequest = new ShardSearchTransportRequest();
            readRequest.readFrom(in);
            assertEquals(0, in.available());
            IllegalStateException illegalStateException = expectThrows(IllegalStateException.class, () -> readRequest.filteringAliases());
            assertEquals("alias filter for aliases: [JSOOSFfZxE, UjLlLkjwWh, uBpgtwuqDG] must be rewritten first",
                illegalStateException.getMessage());
            IndexMetaData.Builder indexMetadata = new IndexMetaData.Builder(baseMetaData)
                .putAlias(AliasMetaData.newAliasMetaDataBuilder("JSOOSFfZxE").filter("{\"term\" : {\"foo\" : \"bar\"}}"))
                .putAlias(AliasMetaData.newAliasMetaDataBuilder("UjLlLkjwWh").filter("{\"term\" : {\"foo\" : \"bar1\"}}"))
                .putAlias(AliasMetaData.newAliasMetaDataBuilder("uBpgtwuqDG").filter("{\"term\" : {\"foo\" : \"bar2\"}}"));
            IndexSettings indexSettings = new IndexSettings(indexMetadata.build(), Settings.EMPTY);
            final long nowInMillis = randomNonNegativeLong();
            QueryShardContext context = new QueryShardContext(
                0, indexSettings, null, null, null, null, null, xContentRegistry(), null, null, () -> nowInMillis);
            readRequest.rewrite(context);
            QueryBuilder queryBuilder = readRequest.filteringAliases();
            assertEquals(queryBuilder, QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("foo", "bar"))
                .should(QueryBuilders.termQuery("foo", "bar1"))
                .should(QueryBuilders.termQuery("foo", "bar2"))
            );
            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(Version.V_5_0_0);
            readRequest.writeTo(output);
            assertEquals(output.bytes().toBytesRef(), requestBytes.toBytesRef());
        }
    }

    // BWC test for changes from #21393
    public void testSerialize50RequestForIndexBoost() throws IOException {
        BytesArray requestBytes = new BytesArray(Base64.getDecoder()
            // this is a base64 encoded request generated with the same input
            .decode("AAZpbmRleDEWTjEyM2trbHFUT21XZDY1Z2VDYlo5ZwABBAABAAIA/wD/////DwABBmluZGV4MUAAAAAAAAAAAP////8PAAAAAAAAAgAAAA" +
                "AAAPa/q8mOKwIAJg=="));

        try (StreamInput in = new NamedWriteableAwareStreamInput(requestBytes.streamInput(), namedWriteableRegistry)) {
            in.setVersion(Version.V_5_0_0);
            ShardSearchTransportRequest readRequest = new ShardSearchTransportRequest();
            readRequest.readFrom(in);
            assertEquals(0, in.available());
            assertEquals(2.0f, readRequest.indexBoost(), 0);

            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(Version.V_5_0_0);
            readRequest.writeTo(output);
            assertEquals(output.bytes().toBytesRef(), requestBytes.toBytesRef());
        }
    }
}
