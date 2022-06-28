/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.enrich.MatchProcessorTests.mapOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class EnrichCacheTests extends ESTestCase {

    public void testCaching() {
        // Emulate cluster metadata:
        // (two enrich indices with corresponding alias entries)
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(EnrichPolicy.getBaseName("policy1") + "-1")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName("policy1")).build())
            )
            .put(
                IndexMetadata.builder(EnrichPolicy.getBaseName("policy2") + "-1")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName("policy2")).build())
            )
            .build();

        // Emulated search requests that an enrich processor could generate:
        // (two unique searches for two enrich policies)
        SearchRequest searchRequest1 = new SearchRequest(EnrichPolicy.getBaseName("policy1")).source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("match_field", "1"))
        );
        SearchRequest searchRequest2 = new SearchRequest(EnrichPolicy.getBaseName("policy1")).source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("match_field", "2"))
        );
        SearchRequest searchRequest3 = new SearchRequest(EnrichPolicy.getBaseName("policy2")).source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("match_field", "1"))
        );
        SearchRequest searchRequest4 = new SearchRequest(EnrichPolicy.getBaseName("policy2")).source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("match_field", "2"))
        );
        // Emulated search response (content doesn't matter, since it isn't used, it just a cache entry)
        List<Map<?, ?>> searchResponse = Collections.singletonList(mapOf("test", "entry"));

        EnrichCache enrichCache = new EnrichCache(3);
        enrichCache.setMetadata(metadata);
        enrichCache.put(searchRequest1, searchResponse);
        enrichCache.put(searchRequest2, searchResponse);
        enrichCache.put(searchRequest3, searchResponse);
        EnrichStatsAction.Response.CacheStats cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.getCount(), equalTo(3L));
        assertThat(cacheStats.getHits(), equalTo(0L));
        assertThat(cacheStats.getMisses(), equalTo(0L));
        assertThat(cacheStats.getEvictions(), equalTo(0L));

        assertThat(enrichCache.get(searchRequest1), notNullValue());
        assertThat(enrichCache.get(searchRequest2), notNullValue());
        assertThat(enrichCache.get(searchRequest3), notNullValue());
        assertThat(enrichCache.get(searchRequest4), nullValue());
        cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.getCount(), equalTo(3L));
        assertThat(cacheStats.getHits(), equalTo(3L));
        assertThat(cacheStats.getMisses(), equalTo(1L));
        assertThat(cacheStats.getEvictions(), equalTo(0L));

        enrichCache.put(searchRequest4, searchResponse);
        cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.getCount(), equalTo(3L));
        assertThat(cacheStats.getHits(), equalTo(3L));
        assertThat(cacheStats.getMisses(), equalTo(1L));
        assertThat(cacheStats.getEvictions(), equalTo(1L));

        // Simulate enrich policy execution, which should make current cache entries unused.
        metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(EnrichPolicy.getBaseName("policy1") + "-2")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName("policy1")).build())
            )
            .put(
                IndexMetadata.builder(EnrichPolicy.getBaseName("policy2") + "-2")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName("policy2")).build())
            )
            .build();
        enrichCache.setMetadata(metadata);

        // Because enrich index has changed, cache can't serve cached entries
        assertThat(enrichCache.get(searchRequest1), nullValue());
        assertThat(enrichCache.get(searchRequest2), nullValue());
        assertThat(enrichCache.get(searchRequest3), nullValue());
        assertThat(enrichCache.get(searchRequest4), nullValue());

        // Add new entries using new enrich index name as key
        enrichCache.put(searchRequest1, searchResponse);
        enrichCache.put(searchRequest2, searchResponse);
        enrichCache.put(searchRequest3, searchResponse);

        // Entries can now be served:
        assertThat(enrichCache.get(searchRequest1), notNullValue());
        assertThat(enrichCache.get(searchRequest2), notNullValue());
        assertThat(enrichCache.get(searchRequest3), notNullValue());
        assertThat(enrichCache.get(searchRequest4), nullValue());
        cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.getCount(), equalTo(3L));
        assertThat(cacheStats.getHits(), equalTo(6L));
        assertThat(cacheStats.getMisses(), equalTo(6L));
        assertThat(cacheStats.getEvictions(), equalTo(4L));
    }

    public void testDeepCopy() {
        Map<String, Object> original = new HashMap<>();
        {
            original.put("foo", "bar");
            original.put("int", 123);
            original.put("double", 123.0D);
            Map<String, Object> innerObject = new HashMap<>();
            innerObject.put("buzz", "hello world");
            innerObject.put("foo_null", null);
            innerObject.put("1", "bar");
            innerObject.put("long", 123L);
            List<String> innerInnerList = new ArrayList<>();
            innerInnerList.add("item1");
            List<Object> innerList = new ArrayList<>();
            innerList.add(innerInnerList);
            innerObject.put("list", innerList);
            original.put("fizz", innerObject);
            List<Map<String, Object>> list = new ArrayList<>();
            Map<String, Object> value = new HashMap<>();
            value.put("field", "value");
            list.add(value);
            list.add(null);
            original.put("list", list);
            List<String> list2 = new ArrayList<>();
            list2.add("foo");
            list2.add("bar");
            list2.add("baz");
            original.put("list2", list2);
        }

        Map<?, ?> result = EnrichCache.deepCopy(original, false);
        assertThat(result, equalTo(original));
        assertThat(result, not(sameInstance(original)));

        result = EnrichCache.deepCopy(original, true);
        assertThat(result, equalTo(original));
        assertThat(result, not(sameInstance(original)));
        Map<?, ?> innerMap = (Map<?, ?>) result.get("fizz");
        expectThrows(UnsupportedOperationException.class, () -> innerMap.remove("x"));
        List<?> innerList = (List<?>) result.get("list");
        expectThrows(UnsupportedOperationException.class, () -> innerList.remove(0));

        original.put("embedded_object", new byte[] { 1, 2, 3 });
        result = EnrichCache.deepCopy(original, false);
        assertArrayEquals(new byte[] { 1, 2, 3 }, (byte[]) result.get("embedded_object"));
    }

}
