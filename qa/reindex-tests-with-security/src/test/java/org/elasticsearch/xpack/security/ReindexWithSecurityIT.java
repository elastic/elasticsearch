/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.BulkIndexByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class ReindexWithSecurityIT extends SecurityIntegTestCase {

    private boolean useSecurity3;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        useSecurity3 = randomBoolean();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ReindexPlugin.class);
        return Collections.unmodifiableCollection(plugins);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ReindexPlugin.class);
        return Collections.unmodifiableCollection(plugins);
    }

    @Override
    protected Settings externalClusterClientSettings() {
        Settings.Builder builder = Settings.builder().put(super.externalClusterClientSettings());
        if (useSecurity3) {
            builder.put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME3);
        } else {
            builder.put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME4);
        }
        builder.put(Security.USER_SETTING.getKey(), "test_admin:changeme");
        return builder.build();
    }

    public void testDeleteByQuery() {
        createIndices("test1", "test2", "test3");

        BulkIndexByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client()).source("test1", "test2").get();
        assertNotNull(response);

        response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client()).source("test*").get();
        assertNotNull(response);

        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> DeleteByQueryAction.INSTANCE.newRequestBuilder(client()).source("test1", "index1").get());
        assertEquals("no such index", e.getMessage());
    }

    public void testUpdateByQuery() {
        createIndices("test1", "test2", "test3");

        BulkIndexByScrollResponse response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client()).source("test1", "test2").get();
        assertNotNull(response);

        response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client()).source("test*").get();
        assertNotNull(response);

        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> UpdateByQueryAction.INSTANCE.newRequestBuilder(client()).source("test1", "index1").get());
        assertEquals("no such index", e.getMessage());
    }

    public void testReindex() {
        createIndices("test1", "test2", "test3", "dest");

        BulkIndexByScrollResponse response = ReindexAction.INSTANCE.newRequestBuilder(client()).source("test1", "test2")
                .destination("dest").get();
        assertNotNull(response);

        response = ReindexAction.INSTANCE.newRequestBuilder(client()).source("test*").destination("dest").get();
        assertNotNull(response);

        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> ReindexAction.INSTANCE.newRequestBuilder(client()).source("test1", "index1").destination("dest").get());
        assertEquals("no such index", e.getMessage());
    }

    private void createIndices(String... indices) {
        if (randomBoolean()) {
            //no aliases
            createIndex(indices);
        } else {
            if (randomBoolean()) {
                //one alias per index with suffix "-alias"
                for (String index : indices) {
                    client().admin().indices().prepareCreate(index).setSettings(indexSettings()).addAlias(new Alias(index + "-alias"));
                }
            } else {
                //same alias pointing to all indices
                for (String index : indices) {
                    client().admin().indices().prepareCreate(index).setSettings(indexSettings()).addAlias(new Alias("alias"));
                }
            }
        }

        for (String index : indices) {
            client().prepareIndex(index, "type").setSource("field", "value").get();
        }
        refresh();
    }
}
