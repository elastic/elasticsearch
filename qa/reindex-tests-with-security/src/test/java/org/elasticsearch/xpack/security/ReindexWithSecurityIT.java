/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SecurityIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class ReindexWithSecurityIT extends SecurityIntegTestCase {

    @Override
    protected Settings externalClusterClientSettings() {
        Settings.Builder builder = Settings.builder().put(super.externalClusterClientSettings());
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME4);
        builder.put(Security.USER_SETTING.getKey(), "test_admin:x-pack-test-password");
        return builder.build();
    }

    public void testDeleteByQuery() {
        createIndicesWithRandomAliases("test1", "test2", "test3");

        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client())
                .source("test1", "test2")
                .filter(QueryBuilders.matchAllQuery())
                .get();
        assertNotNull(response);

        response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client())
                .source("test*")
                .filter(QueryBuilders.matchAllQuery())
                .get();
        assertNotNull(response);

        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> DeleteByQueryAction.INSTANCE.newRequestBuilder(client())
                        .source("test1", "index1")
                        .filter(QueryBuilders.matchAllQuery())
                        .get());
        assertEquals("no such index", e.getMessage());
    }

    public void testUpdateByQuery() {
        createIndicesWithRandomAliases("test1", "test2", "test3");

        BulkByScrollResponse response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client()).source("test1", "test2").get();
        assertNotNull(response);

        response = UpdateByQueryAction.INSTANCE.newRequestBuilder(client()).source("test*").get();
        assertNotNull(response);

        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> UpdateByQueryAction.INSTANCE.newRequestBuilder(client()).source("test1", "index1").get());
        assertEquals("no such index", e.getMessage());
    }

    public void testReindex() {
        createIndicesWithRandomAliases("test1", "test2", "test3", "dest");

        BulkByScrollResponse response = ReindexAction.INSTANCE.newRequestBuilder(client()).source("test1", "test2")
                .destination("dest").get();
        assertNotNull(response);

        response = ReindexAction.INSTANCE.newRequestBuilder(client()).source("test*").destination("dest").get();
        assertNotNull(response);

        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> ReindexAction.INSTANCE.newRequestBuilder(client()).source("test1", "index1").destination("dest").get());
        assertEquals("no such index", e.getMessage());
    }
}
