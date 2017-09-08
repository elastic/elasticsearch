/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.analysis.catalog.EsCatalog;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.hasSize;

public class SqlGetIndicesActionTests extends ESTestCase {
    private final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(Settings.EMPTY);
    private final AtomicBoolean called = new AtomicBoolean(false);
    private final AtomicReference<Exception> error = new AtomicReference<>();

    public void testOperation() throws IOException {
        SqlGetIndicesAction.Request request = new SqlGetIndicesAction.Request(IndicesOptions.lenientExpandOpen(), "test", "bar", "foo*");
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                        .put(index("test"))
                        .put(index("foo1"))
                        .put(index("foo2")))
                .build();
        ActionListener<SqlGetIndicesAction.Response> listener = new ActionListener<SqlGetIndicesAction.Response>() {
            @Override
            public void onResponse(SqlGetIndicesAction.Response response) {
                assertThat(response.indices(), hasSize(3));
                assertEquals("foo1", response.indices().get(0).name());
                assertEquals("foo2", response.indices().get(1).name());
                assertEquals("test", response.indices().get(2).name());
                called.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
            }
        };
        SqlGetIndicesAction.operation(indexNameExpressionResolver, EsCatalog::new, request, clusterState, listener);
        if (error.get() != null) {
            throw new AssertionError(error.get());
        }
        assertTrue(called.get());
    }

    public void testMultipleTypes() throws IOException {
        SqlGetIndicesAction.Request request = new SqlGetIndicesAction.Request(IndicesOptions.lenientExpandOpen(), "foo*");
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                        .put(index("foo1"))
                        .put(index("foo2").putMapping("test2", "{}")))
                .build();
        final AtomicBoolean called = new AtomicBoolean(false);
        final AtomicReference<Exception> error = new AtomicReference<>();
        ActionListener<SqlGetIndicesAction.Response> listener = new ActionListener<SqlGetIndicesAction.Response>() {
            @Override
            public void onResponse(SqlGetIndicesAction.Response response) {
                assertThat(response.indices(), hasSize(1));
                assertEquals("foo1", response.indices().get(0).name());
                called.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
            }
        };
        SqlGetIndicesAction.operation(indexNameExpressionResolver, EsCatalog::new, request, clusterState, listener);
        if (error.get() != null) {
            throw new AssertionError(error.get());
        }
        assertTrue(called.get());
    }


    IndexMetaData.Builder index(String name) throws IOException {
        return IndexMetaData.builder(name)
                .settings(Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(SETTING_VERSION_CREATED, Version.CURRENT))
                .putMapping("test", "{}");
    }
}
