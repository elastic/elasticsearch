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

package org.elasticsearch.test;

import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.merge.policy.*;
import org.elasticsearch.index.merge.scheduler.ConcurrentMergeSchedulerProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerModule;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.merge.scheduler.SerialMergeSchedulerProvider;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.search.SearchService;

import java.util.Arrays;
import java.util.Random;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllFilesClosed;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSearchersClosed;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Base test cluster that exposes the basis to run tests against any elasticsearch cluster, whose layout
 * (e.g. number of nodes) is predefined and cannot be changed during the tests execution
 */
public abstract class ImmutableTestCluster implements Iterable<Client> {

    private final ESLogger logger = Loggers.getLogger(getClass());

    /**
     * Key used to retrieve the index random seed from the index settings on a running node.
     * The value of this seed can be used to initialize a random context for a specific index.
     * It's set once per test via a generic index template.
     */
    public static final String SETTING_INDEX_SEED = "index.tests.seed";

    public static final int DEFAULT_MIN_NUM_SHARDS = 1;
    public static final int DEFAULT_MAX_NUM_SHARDS = 10;

    protected Random random;

    protected double transportClientRatio = 0.0;

    /**
     * This method should be executed before each test to reset the cluster to its initial state.
     */
    public void beforeTest(Random random, double transportClientRatio) {
        assert transportClientRatio >= 0.0 && transportClientRatio <= 1.0;
        logger.debug("Reset test cluster with transport client ratio: [{}]", transportClientRatio);
        this.transportClientRatio = transportClientRatio;
        this.random = new Random(random.nextLong());
    }

    /**
     * Wipes any data that a test can leave behind: indices, templates and repositories
     */
    public void wipe() {
        wipeIndices("_all");
        wipeTemplates();
        wipeRepositories();
    }

    /**
     * This method checks all the things that need to be checked after each test
     */
    public void assertAfterTest() {
        assertAllSearchersClosed();
        assertAllFilesClosed();
        ensureEstimatedStats();
    }

    /**
     * This method should be executed during tear down, after each test (but after assertAfterTest)
     */
    public abstract void afterTest();

    /**
     * Returns a client connected to any node in the cluster
     */
    public abstract Client client();

    /**
     * Returns the number of nodes in the cluster.
     */
    public abstract int size();

    /**
     * Returns the number of data nodes in the cluster.
     */
    public abstract int dataNodes();

    /**
     * Closes the current cluster
     */
    public abstract void close();

    /**
     * Deletes the given indices from the tests cluster. If no index name is passed to this method
     * all indices are removed.
     */
    public void wipeIndices(String... indices) {
        assert indices != null && indices.length > 0;
        if (size() > 0) {
            try {
                assertAcked(client().admin().indices().prepareDelete(indices));
            } catch (IndexMissingException e) {
                // ignore
            } catch (ElasticsearchIllegalArgumentException e) {
                // Happens if `action.destructive_requires_name` is set to true
                // which is the case in the CloseIndexDisableCloseAllTests
                if ("_all".equals(indices[0])) {
                    ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().execute().actionGet();
                    ObjectArrayList<String> concreteIndices = new ObjectArrayList<String>();
                    for (IndexMetaData indexMetaData : clusterStateResponse.getState().metaData()) {
                        concreteIndices.add(indexMetaData.getIndex());
                    }
                    if (!concreteIndices.isEmpty()) {
                        assertAcked(client().admin().indices().prepareDelete(concreteIndices.toArray(String.class)));
                    }
                }
            }
        }
    }

    /**
     * Deletes index templates, support wildcard notation.
     * If no template name is passed to this method all templates are removed.
     */
    public void wipeTemplates(String... templates) {
        if (size() > 0) {
            // if nothing is provided, delete all
            if (templates.length == 0) {
                templates = new String[]{"*"};
            }
            for (String template : templates) {
                try {
                    client().admin().indices().prepareDeleteTemplate(template).execute().actionGet();
                } catch (IndexTemplateMissingException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Deletes repositories, supports wildcard notation.
     */
    public void wipeRepositories(String... repositories) {
        if (size() > 0) {
            // if nothing is provided, delete all
            if (repositories.length == 0) {
                repositories = new String[]{"*"};
            }
            for (String repository : repositories) {
                try {
                    client().admin().cluster().prepareDeleteRepository(repository).execute().actionGet();
                } catch (RepositoryMissingException ex) {
                    // ignore
                }
            }
        }
    }

    /**
     * Ensures that the breaker statistics are reset to 0 since we wiped all indices and that
     * means all stats should be set to 0 otherwise something is wrong with the field data
     * calculation.
     */
    public void ensureEstimatedStats() {
        if (size() > 0) {
            NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats()
                    .clear().setBreaker(true).execute().actionGet();
            for (NodeStats stats : nodeStats.getNodes()) {
                assertThat("Breaker not reset to 0 on node: " + stats.getNode(),
                        stats.getBreaker().getEstimated(), equalTo(0L));
            }
        }
    }

    /**
     * Creates a randomized index template. This template is used to pass in randomized settings on a
     * per index basis.
     */
    public void randomIndexTemplate() {
        // TODO move settings for random directory etc here into the index based randomized settings.
        if (size() > 0) {
            ImmutableSettings.Builder builder = setRandomNormsLoading(setRandomMerge(random, ImmutableSettings.builder())
                    .put(SETTING_INDEX_SEED, random.nextLong()))
                    .put(SETTING_NUMBER_OF_SHARDS, RandomInts.randomIntBetween(random, DEFAULT_MIN_NUM_SHARDS, DEFAULT_MAX_NUM_SHARDS))
                    .put(SETTING_NUMBER_OF_REPLICAS, RandomInts.randomIntBetween(random, 0, 1));
            client().admin().indices().preparePutTemplate("random_index_template")
                    .setTemplate("*")
                    .setOrder(0)
                    .setSettings(builder)
                    .execute().actionGet();
        }
    }

    private ImmutableSettings.Builder setRandomNormsLoading(ImmutableSettings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(SearchService.NORMS_LOADING_KEY, RandomPicks.randomFrom(random, Arrays.asList(FieldMapper.Loading.EAGER, FieldMapper.Loading.LAZY)));
        }
        return builder;
    }

    private static ImmutableSettings.Builder setRandomMerge(Random random, ImmutableSettings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(AbstractMergePolicyProvider.INDEX_COMPOUND_FORMAT,
                    random.nextBoolean() ? random.nextDouble() : random.nextBoolean());
        }
        Class<? extends MergePolicyProvider<?>> mergePolicy = TieredMergePolicyProvider.class;
        switch (random.nextInt(5)) {
            case 4:
                mergePolicy = LogByteSizeMergePolicyProvider.class;
                break;
            case 3:
                mergePolicy = LogDocMergePolicyProvider.class;
                break;
            case 0:
                mergePolicy = null;
        }
        if (mergePolicy != null) {
            builder.put(MergePolicyModule.MERGE_POLICY_TYPE_KEY, mergePolicy.getName());
        }

        if (random.nextBoolean()) {
            builder.put(MergeSchedulerProvider.FORCE_ASYNC_MERGE, random.nextBoolean());
        }
        switch (random.nextInt(5)) {
            case 4:
                builder.put(MergeSchedulerModule.MERGE_SCHEDULER_TYPE_KEY, SerialMergeSchedulerProvider.class.getName());
                break;
            case 3:
                builder.put(MergeSchedulerModule.MERGE_SCHEDULER_TYPE_KEY, ConcurrentMergeSchedulerProvider.class.getName());
                break;
        }

        return builder;
    }
}
