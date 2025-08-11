/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.SecurityField;

@SuppressWarnings("removal")
public class ReindexWithSecurityIT extends SecurityIntegTestCase {

    @Override
    protected Settings externalClusterClientSettings() {
        Settings.Builder builder = Settings.builder().put(super.externalClusterClientSettings());
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, SecurityField.NAME4);
        builder.put(SecurityField.USER_SETTING.getKey(), "test_admin:x-pack-test-password");
        return builder.build();
    }

    /**
     * TODO: this entire class should be removed. SecurityIntegTestCase is meant for tests, but we run against real xpack
     */
    @Override
    public void doAssertXPackIsInstalled() {
        // this assertion doesn't make sense with a real distribution, since there is not currently a way
        // from nodes info to see which modules are loaded
    }

    public void testDeleteByQuery() {
        createIndicesWithRandomAliases("test1", "test2", "test3");

        BulkByScrollResponse response = new DeleteByQueryRequestBuilder(client(), DeleteByQueryAction.INSTANCE).source("test1", "test2")
            .filter(QueryBuilders.matchAllQuery())
            .get();
        assertNotNull(response);

        response = new DeleteByQueryRequestBuilder(client(), DeleteByQueryAction.INSTANCE).source("test*")
            .filter(QueryBuilders.matchAllQuery())
            .get();
        assertNotNull(response);

        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> new DeleteByQueryRequestBuilder(client(), DeleteByQueryAction.INSTANCE).source("test1", "index1")
                .filter(QueryBuilders.matchAllQuery())
                .get()
        );
        assertEquals("no such index [index1]", e.getMessage());
    }

    public void testUpdateByQuery() {
        createIndicesWithRandomAliases("test1", "test2", "test3");

        BulkByScrollResponse response = new UpdateByQueryRequestBuilder(client(), UpdateByQueryAction.INSTANCE).source("test1", "test2")
            .get();
        assertNotNull(response);

        response = new UpdateByQueryRequestBuilder(client(), UpdateByQueryAction.INSTANCE).source("test*").get();
        assertNotNull(response);

        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> new UpdateByQueryRequestBuilder(client(), UpdateByQueryAction.INSTANCE).source("test1", "index1").get()
        );
        assertEquals("no such index [index1]", e.getMessage());
    }

    public void testReindex() {
        createIndicesWithRandomAliases("test1", "test2", "test3", "dest");

        BulkByScrollResponse response = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source("test1", "test2")
            .destination("dest")
            .get();
        assertNotNull(response);

        response = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source("test*").destination("dest").get();
        assertNotNull(response);

        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source("test1", "index1").destination("dest").get()
        );
        assertEquals("no such index [index1]", e.getMessage());
    }
}
