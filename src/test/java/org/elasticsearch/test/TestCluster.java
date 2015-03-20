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
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.repositories.RepositoryMissingException;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;

/**
 * Base test cluster that exposes the basis to run tests against any elasticsearch cluster, whose layout
 * (e.g. number of nodes) is predefined and cannot be changed during the tests execution
 */
public abstract class TestCluster implements Iterable<Client>, Closeable {

    protected final ESLogger logger = Loggers.getLogger(getClass());
    private final long seed;

    protected Random random;

    protected double transportClientRatio = 0.0;

    public TestCluster(long seed) {
        this.seed = seed;
    }

    public long seed() {
        return seed;
    }

    /**
     * This method should be executed before each test to reset the cluster to its initial state.
     */
    public void beforeTest(Random random, double transportClientRatio) throws IOException {
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
    public void assertAfterTest() throws IOException {
        assertAllSearchersClosed();
        assertAllFilesClosed();
        ensureEstimatedStats();
    }

    /**
     * This method should be executed during tear down, after each test (but after assertAfterTest)
     */
    public abstract void afterTest() throws IOException;

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
    public abstract int numDataNodes();

    /**
     * Returns the number of data and master eligible nodes in the cluster.
     */
    public abstract int numDataAndMasterNodes();

    /**
     * Returns the http addresses of the nodes within the cluster.
     * Can be used to run REST tests against the test cluster.
     */
    public abstract InetSocketAddress[] httpAddresses();

    /**
     * Closes the current cluster
     */
    @Override
    public abstract void close() throws IOException;

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
                    ObjectArrayList<String> concreteIndices = new ObjectArrayList<>();
                    for (IndexMetaData indexMetaData : clusterStateResponse.getState().metaData()) {
                        concreteIndices.add(indexMetaData.getIndex());
                    }
                    if (!concreteIndices.isEmpty()) {
                        assertAcked(client().admin().indices().prepareDelete(concreteIndices.toArray(String.class)));
                    }
                }
            } catch (AssertionError ae) {
                // Try to see what threads are doing when we hit the "Delete index failed - not acked":
                logger.info("dump all threads on AssertionError");
                ElasticsearchTestCase.printStackDump(logger);
                logger.info("done dump all threads on AssertionError");
                throw ae;
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
     * Ensures that any breaker statistics are reset to 0.
     *
     * The implementation is specific to the test cluster, because the act of
     * checking some breaker stats can increase them.
     */
    public abstract void ensureEstimatedStats();

    /**
     * Returns the cluster name
     */
    public abstract String getClusterName();
}
