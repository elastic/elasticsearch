/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.heappressure;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.HeapPressureTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

/**
 * Base class for tests that build large automatons on a {@code wildcard} field. Each subclass provides its own {@code @ClassRule}
 * cluster, because the heap size is fixed at node startup.
 *
 * <p>Request caching is disabled on every search so that each request builds its automaton on every shard rather than being
 * answered from the shard request cache.
 */
public abstract class WildcardFieldHeapPressureTestCase extends HeapPressureTestCase {

    private static final String INDEX = "wildcard-heap-pressure";
    private static final int SHARDS = 5;
    // Set this to true to disable the circuit breaker and validate that the OOM actually happens instead of being caught by the breaker.
    private static final boolean VALIDATE_OOM = false;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .distribution(DistributionType.DEFAULT)
        .module("test-pausable-field")
        .setting("xpack.security.enabled", "false")
        // The ML native controller isn't needed, and fails to start on some machines
        .setting("xpack.ml.enabled", "false")
        // Test nodes run with two processors, which gives a search pool of only four threads. Parked requests hold one thread each, so
        // the pool has to be large enough for their automatons to add up to more than the heap.
        .setting("thread_pool.search.size", "32")
        .jvmArg("-Xms256m")
        .jvmArg("-Xmx256m")
        .apply(c -> {
            if (VALIDATE_OOM) {
                c.setting("indices.breaker.request.limit", "-1");
                c.setting("indices.breaker.total.use_real_memory", "false");
                c.setting("indices.breaker.total.limit", "100gb");
            }
        })
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void createTestIndex() throws IOException {
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(Strings.format("""
            {
              "settings": {"number_of_shards": %d, "number_of_replicas": 0},
              "mappings": {
                "runtime": {"p": {"type": "long", "script": {"source": "", "lang": "pause"}}},
                "properties": {"w": {"type": "wildcard"}}
              }
            }
            """, SHARDS));
        client().performRequest(create);

        // Automatons are built per shard however few documents it holds, so a few per shard is enough.
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < SHARDS * 10; i++) {
            bulk.append("{\"index\":{}}\n{\"w\":\"value-").append(i).append("-abcabcabc\"}\n");
        }
        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);
    }

    /**
     * A wildcard search that also filters on the pausable field {@code p}. The automaton is built with the query, and the script only
     * runs later, when documents are scored, so while the field is blocked every such request keeps its automaton alive and parks a
     * search thread.
     */
    protected static Request pausableWildcardSearch(String pattern) {
        return pausableSearch(Strings.format("""
            {"wildcard": {"w": {"value": "%s"}}}""", pattern));
    }

    /** Like {@link #pausableWildcardSearch}, for a regexp query. */
    protected static Request pausableRegexpSearch(String pattern, int maxDeterminizedStates) {
        return pausableSearch(Strings.format("""
            {"regexp": {"w": {"value": "%s", "max_determinized_states": %d}}}""", pattern, maxDeterminizedStates));
    }

    private static Request pausableSearch(String mustClause) {
        return search(Strings.format("""
            {
              "size": 0,
              "query": {
                "bool": {
                  "must": [%s],
                  "filter": [{"term": {"p": 1}}]
                }
              }
            }
            """, mustClause));
    }

    protected static Request wildcardSearch(String pattern) {
        return search(Strings.format("""
            {"size": 0, "query": {"wildcard": {"w": {"value": "%s"}}}}
            """, pattern));
    }

    protected static Request regexpSearch(String pattern, int maxDeterminizedStates) {
        return search(Strings.format("""
            {"size": 0, "query": {"regexp": {"w": {"value": "%s", "max_determinized_states": %d}}}}
            """, pattern, maxDeterminizedStates));
    }

    private static Request search(String body) {
        Request search = new Request("POST", "/" + INDEX + "/_search");
        search.addParameter("request_cache", "false");
        search.setJsonEntity(body);
        return search;
    }
}
