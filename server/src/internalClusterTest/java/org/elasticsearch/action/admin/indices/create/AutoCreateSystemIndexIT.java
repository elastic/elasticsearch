/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.TestSystemIndexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.snapshots.SystemIndicesSnapshotIT;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_HIDDEN;
import static org.elasticsearch.indices.TestSystemIndexDescriptor.INDEX_NAME;
import static org.elasticsearch.indices.TestSystemIndexDescriptor.PRIMARY_INDEX_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AutoCreateSystemIndexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(), TestSystemIndexPlugin.class),
            UnmanagedSystemIndexTestPlugin.class
        );
    }

    public void testAutoCreatePrimaryIndex() throws Exception {
        CreateIndexRequest request = new CreateIndexRequest(PRIMARY_INDEX_NAME);
        client().execute(AutoCreateAction.INSTANCE, request).get();

        GetIndexResponse response = client().admin().indices().prepareGetIndex().addIndices(PRIMARY_INDEX_NAME).get();
        assertThat(response.indices().length, is(1));
    }

    public void testAutoCreatePrimaryIndexFromAlias() throws Exception {
        CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME);
        client().execute(AutoCreateAction.INSTANCE, request).get();

        GetIndexResponse response = client().admin().indices().prepareGetIndex().addIndices(PRIMARY_INDEX_NAME).get();
        assertThat(response.indices().length, is(1));
    }

    public void testAutoCreateNonPrimaryIndex() throws Exception {
        CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME + "-2");
        client().execute(AutoCreateAction.INSTANCE, request).get();

        GetIndexResponse response = client().admin().indices().prepareGetIndex().addIndices(INDEX_NAME + "-2").get();
        assertThat(response.indices().length, is(1));
    }

    public void testSystemIndicesAutoCreatedAsHidden() throws Exception {
        CreateIndexRequest request = new CreateIndexRequest(UnmanagedSystemIndexTestPlugin.SYSTEM_INDEX_NAME);
        client().execute(AutoCreateAction.INSTANCE, request).get();

        GetIndexResponse response = client().admin()
            .indices()
            .prepareGetIndex()
            .addIndices(UnmanagedSystemIndexTestPlugin.SYSTEM_INDEX_NAME)
            .get();
        assertThat(response.indices().length, is(1));
        Settings settings = response.settings().get(UnmanagedSystemIndexTestPlugin.SYSTEM_INDEX_NAME);
        assertThat(settings, notNullValue());
        assertThat(settings.getAsBoolean(SETTING_INDEX_HIDDEN, false), is(true));
    }

    public void testSystemIndicesAutoCreateRejectedWhenNotHidden() {
        CreateIndexRequest request = new CreateIndexRequest(UnmanagedSystemIndexTestPlugin.SYSTEM_INDEX_NAME);
        request.settings(Settings.builder().put(SETTING_INDEX_HIDDEN, false).build());
        ExecutionException exception = expectThrows(
            ExecutionException.class,
            () -> client().execute(AutoCreateAction.INSTANCE, request).get()
        );

        assertThat(
            exception.getCause().getMessage(),
            containsString("Cannot auto-create system index [.unmanaged-system-idx] with [index.hidden] set to 'false'")
        );
    }

    public static class UnmanagedSystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        public static final String SYSTEM_INDEX_NAME = ".unmanaged-system-idx";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(new SystemIndexDescriptor(SYSTEM_INDEX_NAME + "*", "System indices for tests"));
        }

        @Override
        public String getFeatureName() {
            return SystemIndicesSnapshotIT.SystemIndexTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "A simple test plugin";
        }
    }

}
