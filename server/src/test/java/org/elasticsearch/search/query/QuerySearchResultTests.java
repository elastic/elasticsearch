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

package org.elasticsearch.search.query;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.Version;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalAggregationsTests;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.suggest.SuggestTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

import static java.util.Collections.emptyList;

public class QuerySearchResultTests extends ESTestCase {

    private final NamedWriteableRegistry namedWriteableRegistry;

    public QuerySearchResultTests() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        this.namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    private static QuerySearchResult createTestInstance() throws Exception {
        ShardId shardId = new ShardId("index", "uuid", randomInt());
        QuerySearchResult result = new QuerySearchResult(randomLong(), new SearchShardTarget("node", shardId, null, OriginalIndices.NONE));
        if (randomBoolean()) {
            result.terminatedEarly(randomBoolean());
        }
        TopDocs topDocs = new TopDocs(new TotalHits(randomLongBetween(0, Long.MAX_VALUE), TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
        result.topDocs(new TopDocsAndMaxScore(topDocs, randomBoolean() ? Float.NaN : randomFloat()), new DocValueFormat[0]);
        result.size(randomInt());
        result.from(randomInt());
        if (randomBoolean()) {
            result.suggest(SuggestTests.createTestItem());
        }
        if (randomBoolean()) {
            result.aggregations(InternalAggregationsTests.createTestInstance());
        }
        return result;
    }

    public void testSerialization() throws Exception {
        QuerySearchResult querySearchResult = createTestInstance();
        Version version = VersionUtils.randomVersion(random());
        QuerySearchResult deserialized = copyStreamable(querySearchResult, namedWriteableRegistry, QuerySearchResult::new, version);
        assertEquals(querySearchResult.getRequestId(), deserialized.getRequestId());
        assertNull(deserialized.getSearchShardTarget());
        assertEquals(querySearchResult.topDocs().maxScore, deserialized.topDocs().maxScore, 0f);
        assertEquals(querySearchResult.topDocs().topDocs.totalHits, deserialized.topDocs().topDocs.totalHits);
        assertEquals(querySearchResult.from(), deserialized.from());
        assertEquals(querySearchResult.size(), deserialized.size());
        assertEquals(querySearchResult.hasAggs(), deserialized.hasAggs());
        if (deserialized.hasAggs()) {
            Aggregations aggs = querySearchResult.consumeAggs();
            Aggregations deserializedAggs = deserialized.consumeAggs();
            assertEquals(aggs.asList(), deserializedAggs.asList());
            List<SiblingPipelineAggregator> pipelineAggs = ((InternalAggregations) aggs).getTopLevelPipelineAggregators();
            List<SiblingPipelineAggregator> deserializedPipelineAggs =
                ((InternalAggregations) deserializedAggs).getTopLevelPipelineAggregators();
            assertEquals(pipelineAggs.size(), deserializedPipelineAggs.size());
            for (int i = 0; i < pipelineAggs.size(); i++) {
                SiblingPipelineAggregator pipelineAgg = pipelineAggs.get(i);
                SiblingPipelineAggregator deserializedPipelineAgg = deserializedPipelineAggs.get(i);
                assertArrayEquals(pipelineAgg.bucketsPaths(), deserializedPipelineAgg.bucketsPaths());
                assertEquals(pipelineAgg.name(), deserializedPipelineAgg.name());
            }
        }
        assertEquals(querySearchResult.terminatedEarly(), deserialized.terminatedEarly());
    }

    //TODO update version and rename after backport
    public void testReadFromPre_8_0_0() throws IOException {
        String message = "AAAAAAAAAGQAAAEAAH/AAAAAAQEGc3Rlcm1zBU5DZ0Jl/wABAwHmAQNyYXcFAQAGHAAAAAAAAABzAgZzdGVybXMFZm9tUFoKAAABAwHmAQNy" +
            "YXcFAQAFAgAAAAAAAABzAAAKTVNUbmtmRERlWkcAAAAAAAAAcwAACmViTEtDVlpYS0Y/AAAAAAAAAHMAAApkRVJlQWhlYkdUEgAAAAAAAABzAAAKemVITnR3T" +
            "0d6SyYAAAAAAAAAcwAACllYeEtZRExETnUGc3Rlcm1zBWtzS25xCgAAAQMB5gEDcmF3BQEABQgAAAAAAAAAcwAAClpHaWVrR0t5UmofAAAAAAAAAHMAAAp1eX" +
            "hmbmplU3lHWwAAAAAAAABzAAAKVk5YZlBUSWl0URgAAAAAAAAAcwAACnZ0Y3R2bENwU1A7AAAAAAAAAHMAAApOUnF4Z3F0V1l6AApQbUlaenRDWXhYSQAAAAA" +
            "AAABzAgZzdGVybXMFZm9tUFoKAAABAwHmAQNyYXcFAQAFAgAAAAAAAABzAAAKTVNUbmtmRERlWkcAAAAAAAAAcwAACmViTEtDVlpYS0Y/AAAAAAAAAHMAAApk" +
            "RVJlQWhlYkdUEgAAAAAAAABzAAAKemVITnR3T0d6SyYAAAAAAAAAcwAACllYeEtZRExETnUGc3Rlcm1zBWtzS25xCgAAAQMB5gEDcmF3BQEABQgAAAAAAAAAc" +
            "wAAClpHaWVrR0t5UmofAAAAAAAAAHMAAAp1eXhmbmplU3lHWwAAAAAAAABzAAAKVk5YZlBUSWl0URgAAAAAAAAAcwAACnZ0Y3R2bENwU1A7AAAAAAAAAHMAAA" +
            "pOUnF4Z3F0V1l6AAp2TXZzbnNGVVJsOAAAAAAAAABzAgZzdGVybXMFZm9tUFoKAAABAwHmAQNyYXcFAQAFAgAAAAAAAABzAAAKTVNUbmtmRERlWkcAAAAAAAA" +
            "AcwAACmViTEtDVlpYS0Y/AAAAAAAAAHMAAApkRVJlQWhlYkdUEgAAAAAAAABzAAAKemVITnR3T0d6SyYAAAAAAAAAcwAACllYeEtZRExETnUGc3Rlcm1zBWtz" +
            "S25xCgAAAQMB5gEDcmF3BQEABQgAAAAAAAAAcwAAClpHaWVrR0t5UmofAAAAAAAAAHMAAAp1eXhmbmplU3lHWwAAAAAAAABzAAAKVk5YZlBUSWl0URgAAAAAA" +
            "AAAcwAACnZ0Y3R2bENwU1A7AAAAAAAAAHMAAApOUnF4Z3F0V1l6AApPRFBodEdiVEZlBgAAAAAAAABzAgZzdGVybXMFZm9tUFoKAAABAwHmAQNyYXcFAQAFAg" +
            "AAAAAAAABzAAAKTVNUbmtmRERlWkcAAAAAAAAAcwAACmViTEtDVlpYS0Y/AAAAAAAAAHMAAApkRVJlQWhlYkdUEgAAAAAAAABzAAAKemVITnR3T0d6SyYAAAA" +
            "AAAAAcwAACllYeEtZRExETnUGc3Rlcm1zBWtzS25xCgAAAQMB5gEDcmF3BQEABQgAAAAAAAAAcwAAClpHaWVrR0t5UmofAAAAAAAAAHMAAAp1eXhmbmplU3lH" +
            "WwAAAAAAAABzAAAKVk5YZlBUSWl0URgAAAAAAAAAcwAACnZ0Y3R2bENwU1A7AAAAAAAAAHMAAApOUnF4Z3F0V1l6AApqYXJTY1VLUWtINgAAAAAAAABzAgZzd" +
            "GVybXMFZm9tUFoKAAABAwHmAQNyYXcFAQAFAgAAAAAAAABzAAAKTVNUbmtmRERlWkcAAAAAAAAAcwAACmViTEtDVlpYS0Y/AAAAAAAAAHMAAApkRVJlQWhlYk" +
            "dUEgAAAAAAAABzAAAKemVITnR3T0d6SyYAAAAAAAAAcwAACllYeEtZRExETnUGc3Rlcm1zBWtzS25xCgAAAQMB5gEDcmF3BQEABQgAAAAAAAAAcwAAClpHaWV" +
            "rR0t5UmofAAAAAAAAAHMAAAp1eXhmbmplU3lHWwAAAAAAAABzAAAKVk5YZlBUSWl0URgAAAAAAAAAcwAACnZ0Y3R2bENwU1A7AAAAAAAAAHMAAApOUnF4Z3F0" +
            "V1l6AApLSlViaGhheVNIHQAAAAAAAABzAgZzdGVybXMFZm9tUFoKAAABAwHmAQNyYXcFAQAFAgAAAAAAAABzAAAKTVNUbmtmRERlWkcAAAAAAAAAcwAACmViT" +
            "EtDVlpYS0Y/AAAAAAAAAHMAAApkRVJlQWhlYkdUEgAAAAAAAABzAAAKemVITnR3T0d6SyYAAAAAAAAAcwAACllYeEtZRExETnUGc3Rlcm1zBWtzS25xCgAAAQ" +
            "MB5gEDcmF3BQEABQgAAAAAAAAAcwAAClpHaWVrR0t5UmofAAAAAAAAAHMAAAp1eXhmbmplU3lHWwAAAAAAAABzAAAKVk5YZlBUSWl0URgAAAAAAAAAcwAACnZ" +
            "0Y3R2bENwU1A7AAAAAAAAAHMAAApOUnF4Z3F0V1l6AAptcGRZZnZpSURaAQptYXhfYnVja2V0BW5hbWUxAQdidWNrZXQx/wNyYXcBAQptYXhfYnVja2V0BW5h" +
            "bWUxAQdidWNrZXQx/wNyYXcBAAACAAH/////AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
            "AAAA==";
        byte[] bytes = Base64.getDecoder().decode(message);
        try (NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(bytes), namedWriteableRegistry)) {
            in.setVersion(VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(),
                Version.max(Version.CURRENT.minimumCompatibilityVersion(), VersionUtils.getPreviousVersion(Version.CURRENT))));
            QuerySearchResult querySearchResult = new QuerySearchResult();
            querySearchResult.readFrom(in);
            assertEquals(100, querySearchResult.getRequestId());
            assertTrue(querySearchResult.hasAggs());
            InternalAggregations aggs = (InternalAggregations)querySearchResult.consumeAggs();
            assertEquals(1, aggs.asList().size());
            //top-level pipeline aggs are retrieved as part of InternalAggregations although they were serialized separately
            assertEquals(1, aggs.getTopLevelPipelineAggregators().size());
        }
    }
}
