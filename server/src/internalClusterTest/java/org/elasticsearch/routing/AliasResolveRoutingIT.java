/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.routing;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Priority;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class AliasResolveRoutingIT extends ESIntegTestCase {

    // see https://github.com/elastic/elasticsearch/issues/13278
    public void testSearchClosedWildcardIndex() throws ExecutionException, InterruptedException {
        createIndex("test-0");
        createIndex("test-1");
        ensureGreen();
        client().admin().indices().prepareAliases().addAlias("test-0", "alias-0").addAlias("test-1", "alias-1").get();
        client().admin().indices().prepareClose("test-1").get();
        indexRandom(
            true,
            client().prepareIndex("test-0").setId("1").setSource("field1", "the quick brown fox jumps"),
            client().prepareIndex("test-0").setId("2").setSource("field1", "quick brown"),
            client().prepareIndex("test-0").setId("3").setSource("field1", "quick")
        );
        refresh("test-*");
        assertHitCount(
            client().prepareSearch()
                .setIndices("alias-*")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setQuery(queryStringQuery("quick"))
                .get(),
            3L
        );
    }

    public void testResolveIndexRouting() {
        createIndex("test1");
        createIndex("test2");
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        indicesAdmin().prepareAliases()
            .addAliasAction(AliasActions.add().index("test1").alias("alias"))
            .addAliasAction(AliasActions.add().index("test1").alias("alias10").routing("0"))
            .addAliasAction(AliasActions.add().index("test1").alias("alias110").searchRouting("1,0"))
            .addAliasAction(AliasActions.add().index("test1").alias("alias12").routing("2"))
            .addAliasAction(AliasActions.add().index("test2").alias("alias20").routing("0"))
            .addAliasAction(AliasActions.add().index("test2").alias("alias21").routing("1"))
            .addAliasAction(AliasActions.add().index("test1").alias("alias0").routing("0"))
            .addAliasAction(AliasActions.add().index("test2").alias("alias0").routing("0"))
            .get();

        assertThat(clusterService().state().metadata().resolveIndexRouting(null, "test1"), nullValue());
        assertThat(clusterService().state().metadata().resolveIndexRouting(null, "alias"), nullValue());

        assertThat(clusterService().state().metadata().resolveIndexRouting(null, "test1"), nullValue());
        assertThat(clusterService().state().metadata().resolveIndexRouting(null, "alias10"), equalTo("0"));
        assertThat(clusterService().state().metadata().resolveIndexRouting(null, "alias20"), equalTo("0"));
        assertThat(clusterService().state().metadata().resolveIndexRouting(null, "alias21"), equalTo("1"));
        assertThat(clusterService().state().metadata().resolveIndexRouting("3", "test1"), equalTo("3"));
        assertThat(clusterService().state().metadata().resolveIndexRouting("0", "alias10"), equalTo("0"));

        try {
            clusterService().state().metadata().resolveIndexRouting("1", "alias10");
            fail("should fail");
        } catch (IllegalArgumentException e) {
            // all is well, we can't have two mappings, one provided, and one in the alias
        }

        try {
            clusterService().state().metadata().resolveIndexRouting(null, "alias0");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            // Expected
        }
    }

    public void testResolveSearchRouting() {
        createIndex("test1");
        createIndex("test2");
        createIndex("test3");
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        indicesAdmin().prepareAliases()
            .addAliasAction(AliasActions.add().index("test1").alias("alias"))
            .addAliasAction(AliasActions.add().index("test1").alias("alias10").routing("0"))
            .addAliasAction(AliasActions.add().index("test2").alias("alias20").routing("0"))
            .addAliasAction(AliasActions.add().index("test2").alias("alias21").routing("1"))
            .addAliasAction(AliasActions.add().index("test1").alias("alias0").routing("0"))
            .addAliasAction(AliasActions.add().index("test2").alias("alias0").routing("0"))
            .addAliasAction(AliasActions.add().index("test3").alias("alias3tw").routing("tw "))
            .addAliasAction(AliasActions.add().index("test3").alias("alias3ltw").routing(" ltw "))
            .addAliasAction(AliasActions.add().index("test3").alias("alias3lw").routing(" lw"))
            .get();

        ClusterState state = clusterService().state();
        IndexNameExpressionResolver indexNameExpressionResolver = internalCluster().getInstance(IndexNameExpressionResolver.class);
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, null, "alias"), nullValue());
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, "0,1", "alias"), equalTo(newMap("test1", newSet("0", "1"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, null, "alias10"), equalTo(newMap("test1", newSet("0"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, null, "alias10"), equalTo(newMap("test1", newSet("0"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, "0", "alias10"), equalTo(newMap("test1", newSet("0"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, "1", "alias10"), nullValue());
        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, null, "alias0"),
            equalTo(newMap("test1", newSet("0"), "test2", newSet("0")))
        );

        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, null, new String[] { "alias10", "alias20" }),
            equalTo(newMap("test1", newSet("0"), "test2", newSet("0")))
        );
        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, null, new String[] { "alias10", "alias21" }),
            equalTo(newMap("test1", newSet("0"), "test2", newSet("1")))
        );
        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, null, new String[] { "alias20", "alias21" }),
            equalTo(newMap("test2", newSet("0", "1")))
        );
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, null, new String[] { "test1", "alias10" }), nullValue());
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, null, new String[] { "alias10", "test1" }), nullValue());

        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, "0", new String[] { "alias10", "alias20" }),
            equalTo(newMap("test1", newSet("0"), "test2", newSet("0")))
        );
        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, "0,1", new String[] { "alias10", "alias20" }),
            equalTo(newMap("test1", newSet("0"), "test2", newSet("0")))
        );
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, "1", new String[] { "alias10", "alias20" }), nullValue());
        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, "0", new String[] { "alias10", "alias21" }),
            equalTo(newMap("test1", newSet("0")))
        );
        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, "1", new String[] { "alias10", "alias21" }),
            equalTo(newMap("test2", newSet("1")))
        );
        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, "0,1,2", new String[] { "alias10", "alias21" }),
            equalTo(newMap("test1", newSet("0"), "test2", newSet("1")))
        );
        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, "0,1,2", new String[] { "test1", "alias10", "alias21" }),
            equalTo(newMap("test1", newSet("0", "1", "2"), "test2", newSet("1")))
        );

        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, "tw , ltw , lw", "test1"),
            equalTo(newMap("test1", newSet("tw ", " ltw ", " lw")))
        );
        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, "tw , ltw , lw", "alias3tw"),
            equalTo(newMap("test3", newSet("tw ")))
        );
        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, "tw , ltw , lw", "alias3ltw"),
            equalTo(newMap("test3", newSet(" ltw ")))
        );
        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, "tw , ltw , lw", "alias3lw"),
            equalTo(newMap("test3", newSet(" lw")))
        );
        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, "0,tw , ltw , lw", "test1", "alias3ltw"),
            equalTo(newMap("test1", newSet("0", "tw ", " ltw ", " lw"), "test3", newSet(" ltw ")))
        );

        assertThat(
            indexNameExpressionResolver.resolveSearchRouting(state, "0,1,2,tw , ltw , lw", (String[]) null),
            equalTo(
                newMap(
                    "test1",
                    newSet("0", "1", "2", "tw ", " ltw ", " lw"),
                    "test2",
                    newSet("0", "1", "2", "tw ", " ltw ", " lw"),
                    "test3",
                    newSet("0", "1", "2", "tw ", " ltw ", " lw")
                )
            )
        );

        assertThat(
            IndexNameExpressionResolver.resolveSearchRoutingAllIndices(state.metadata(), "0,1,2,tw , ltw , lw"),
            equalTo(
                newMap(
                    "test1",
                    newSet("0", "1", "2", "tw ", " ltw ", " lw"),
                    "test2",
                    newSet("0", "1", "2", "tw ", " ltw ", " lw"),
                    "test3",
                    newSet("0", "1", "2", "tw ", " ltw ", " lw")
                )
            )
        );
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private <T> Set<T> newSet(T... elements) {
        return newHashSet(elements);
    }

    private <K, V> Map<K, V> newMap(K key, V value) {
        Map<K, V> r = new HashMap<>();
        r.put(key, value);
        return r;
    }

    private <K, V> Map<K, V> newMap(K key1, V value1, K key2, V value2) {
        Map<K, V> r = new HashMap<>();
        r.put(key1, value1);
        r.put(key2, value2);
        return r;
    }

    private <K, V> Map<K, V> newMap(K key1, V value1, K key2, V value2, K key3, V value3) {
        Map<K, V> r = new HashMap<>();
        r.put(key1, value1);
        r.put(key2, value2);
        r.put(key3, value3);
        return r;
    }

}
