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

package org.elasticsearch.routing;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Priority;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.AliasAction.newAddAliasAction;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class AliasResolveRoutingIT extends ESIntegTestCase {
    public void testResolveIndexRouting() throws Exception {
        createIndex("test1");
        createIndex("test2");
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias")).execute().actionGet();
        client().admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias10").routing("0")).execute().actionGet();
        client().admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias110").searchRouting("1,0")).execute().actionGet();
        client().admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias12").routing("2")).execute().actionGet();
        client().admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test2", "alias20").routing("0")).execute().actionGet();
        client().admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test2", "alias21").routing("1")).execute().actionGet();
        client().admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias0").routing("0")).execute().actionGet();
        client().admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test2", "alias0").routing("0")).execute().actionGet();

        assertThat(clusterService().state().metaData().resolveIndexRouting(null, null, "test1"), nullValue());
        assertThat(clusterService().state().metaData().resolveIndexRouting(null, null, "alias"), nullValue());

        assertThat(clusterService().state().metaData().resolveIndexRouting(null, null, "test1"), nullValue());
        assertThat(clusterService().state().metaData().resolveIndexRouting(null, null, "alias10"), equalTo("0"));
        assertThat(clusterService().state().metaData().resolveIndexRouting(null, null, "alias20"), equalTo("0"));
        assertThat(clusterService().state().metaData().resolveIndexRouting(null, null, "alias21"), equalTo("1"));
        assertThat(clusterService().state().metaData().resolveIndexRouting(null, "3", "test1"), equalTo("3"));
        assertThat(clusterService().state().metaData().resolveIndexRouting(null, "0", "alias10"), equalTo("0"));

        // Force the alias routing and ignore the parent.
        assertThat(clusterService().state().metaData().resolveIndexRouting("1", null, "alias10"), equalTo("0"));
        try {
            clusterService().state().metaData().resolveIndexRouting(null, "1", "alias10");
            fail("should fail");
        } catch (IllegalArgumentException e) {
            // all is well, we can't have two mappings, one provided, and one in the alias
        }

        try {
            clusterService().state().metaData().resolveIndexRouting(null, null, "alias0");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            // Expected
        }
    }

    public void testResolveSearchRouting() throws Exception {
        createIndex("test1");
        createIndex("test2");
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias")).execute().actionGet();
        client().admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias10").routing("0")).execute().actionGet();
        client().admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test2", "alias20").routing("0")).execute().actionGet();
        client().admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test2", "alias21").routing("1")).execute().actionGet();
        client().admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test1", "alias0").routing("0")).execute().actionGet();
        client().admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test2", "alias0").routing("0")).execute().actionGet();

        ClusterState state = clusterService().state();
        IndexNameExpressionResolver indexNameExpressionResolver = internalCluster().getInstance(IndexNameExpressionResolver.class);
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, null, "alias"), nullValue());
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, "0,1", "alias"), equalTo(newMap("test1", newSet("0", "1"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, null, "alias10"), equalTo(newMap("test1", newSet("0"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, null, "alias10"), equalTo(newMap("test1", newSet("0"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, "0", "alias10"), equalTo(newMap("test1", newSet("0"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, "1", "alias10"), nullValue());
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, null, "alias0"), equalTo(newMap("test1", newSet("0"), "test2", newSet("0"))));

        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, null, new String[]{"alias10", "alias20"}),
                equalTo(newMap("test1", newSet("0"), "test2", newSet("0"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, null, new String[]{"alias10", "alias21"}),
                equalTo(newMap("test1", newSet("0"), "test2", newSet("1"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, null, new String[]{"alias20", "alias21"}),
                equalTo(newMap("test2", newSet("0", "1"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, null, new String[]{"test1", "alias10"}), nullValue());
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, null, new String[]{"alias10", "test1"}), nullValue());


        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, "0", new String[]{"alias10", "alias20"}),
                equalTo(newMap("test1", newSet("0"), "test2", newSet("0"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, "0,1", new String[]{"alias10", "alias20"}),
                equalTo(newMap("test1", newSet("0"), "test2", newSet("0"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, "1", new String[]{"alias10", "alias20"}), nullValue());
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, "0", new String[]{"alias10", "alias21"}),
                equalTo(newMap("test1", newSet("0"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, "1", new String[]{"alias10", "alias21"}),
                equalTo(newMap("test2", newSet("1"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, "0,1,2", new String[]{"alias10", "alias21"}),
                equalTo(newMap("test1", newSet("0"), "test2", newSet("1"))));
        assertThat(indexNameExpressionResolver.resolveSearchRouting(state, "0,1,2", new String[]{"test1", "alias10", "alias21"}),
                equalTo(newMap("test1", newSet("0", "1", "2"), "test2", newSet("1"))));
    }

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

}
