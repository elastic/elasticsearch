/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.repositories.RepositoryMissingException;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Base test cluster that exposes the basis to run tests against any elasticsearch cluster, whose layout
 * (e.g. number of nodes) is predefined and cannot be changed during the tests execution
 */
public abstract class TestCluster implements Closeable {

    protected final Logger logger = LogManager.getLogger(getClass());
    private final long seed;

    protected Random random;

    public TestCluster(long seed) {
        this.seed = seed;
    }

    public long seed() {
        return seed;
    }

    /**
     * This method should be executed before each test to reset the cluster to its initial state.
     */
    public void beforeTest(Random randomGenerator) throws IOException, InterruptedException {
        this.random = new Random(randomGenerator.nextLong());
    }

    /**
     * Wipes any data that a test can leave behind: indices, templates (except exclude templates) and repositories
     */
    public void wipe(Set<String> excludeTemplates) {
        // First delete data streams, because composable index templates can't be deleted if these templates are still used by data streams.
        wipeAllDataStreams();
        wipeAllComposableIndexTemplates(excludeTemplates);
        wipeAllComponentTemplates(excludeTemplates);

        wipeIndices("_all");
        wipeAllTemplates(excludeTemplates);
        wipeRepositories();
    }

    /**
     * Assertions that should run before the cluster is wiped should be called in this method
     */
    public void beforeIndexDeletion() throws Exception {}

    /**
     * This method checks all the things that need to be checked after each test
     */
    public void assertAfterTest() throws Exception {
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
                // include wiping hidden indices!
                assertAcked(
                    client().admin()
                        .indices()
                        .prepareDelete(indices)
                        .setIndicesOptions(IndicesOptions.fromOptions(false, true, true, true, true, false, false, true, false))
                );
            } catch (IndexNotFoundException e) {
                // ignore
            } catch (IllegalArgumentException e) {
                // Happens if `action.destructive_requires_name` is set to true
                // which is the case in the CloseIndexDisableCloseAllTests
                if ("_all".equals(indices[0])) {
                    ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().execute().actionGet();
                    ArrayList<String> concreteIndices = new ArrayList<>();
                    for (IndexMetadata indexMetadata : clusterStateResponse.getState().metadata()) {
                        concreteIndices.add(indexMetadata.getIndex().getName());
                    }
                    if (concreteIndices.isEmpty() == false) {
                        assertAcked(client().admin().indices().prepareDelete(concreteIndices.toArray(new String[0])));
                    }
                }
            }
        }
    }

    /**
     * Removes all templates, except the templates defined in the exclude
     */
    public void wipeAllTemplates(Set<String> exclude) {
        if (size() > 0) {
            GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates().get();
            for (IndexTemplateMetadata indexTemplate : response.getIndexTemplates()) {
                if (exclude.contains(indexTemplate.getName())) {
                    continue;
                }
                try {
                    client().admin().indices().prepareDeleteTemplate(indexTemplate.getName()).execute().actionGet();
                } catch (IndexTemplateMissingException e) {
                    // ignore
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
                templates = new String[] { "*" };
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
                repositories = new String[] { "*" };
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

    public void wipeAllDataStreams() {
        if (size() > 0) {
            var request = new DeleteDataStreamAction.Request("*");
            request.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN);
            try {
                assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, request).actionGet());
            } catch (IllegalStateException e) {
                // Ignore if action isn't registered, because data streams is a module and
                // if the delete action isn't registered then there no data streams to delete.
                if (e.getMessage().startsWith("failed to find action") == false) {
                    throw e;
                }
            }
        }
    }

    public void wipeAllComposableIndexTemplates(Set<String> excludeTemplates) {
        if (size() > 0) {
            var templates = client().execute(GetComposableIndexTemplateAction.INSTANCE, new GetComposableIndexTemplateAction.Request("*"))
                .actionGet()
                .indexTemplates()
                .keySet()
                .stream()
                .filter(template -> excludeTemplates.contains(template) == false)
                .toArray(String[]::new);

            if (templates.length != 0) {
                var request = new DeleteComposableIndexTemplateAction.Request(templates);
                assertAcked(client().execute(DeleteComposableIndexTemplateAction.INSTANCE, request).actionGet());
            }
        }
    }

    public void wipeAllComponentTemplates(Set<String> excludeTemplates) {
        if (size() > 0) {
            var templates = client().execute(GetComponentTemplateAction.INSTANCE, new GetComponentTemplateAction.Request("*"))
                .actionGet()
                .getComponentTemplates()
                .keySet()
                .stream()
                .filter(template -> excludeTemplates.contains(template) == false)
                .toArray(String[]::new);

            if (templates.length != 0) {
                var request = new DeleteComponentTemplateAction.Request(templates);
                assertAcked(client().execute(DeleteComponentTemplateAction.INSTANCE, request).actionGet());
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

    /**
     * Returns an {@link Iterable} over all clients in this test cluster
     */
    public abstract Iterable<Client> getClients();

    /**
     * Returns this clusters {@link NamedWriteableRegistry} this is needed to
     * deserialize binary content from this cluster that might include custom named writeables
     */
    public abstract NamedWriteableRegistry getNamedWriteableRegistry();
}
