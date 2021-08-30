/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.indices.TestSystemIndexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;

import static org.elasticsearch.indices.TestSystemIndexDescriptor.INDEX_NAME;
import static org.elasticsearch.indices.TestSystemIndexDescriptor.PRIMARY_INDEX_NAME;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AutoCreateSystemIndexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestSystemIndexPlugin.class);
    }

    public void testAutoCreatePrimaryIndex() throws Exception {
        CreateIndexRequest request = new CreateIndexRequest(PRIMARY_INDEX_NAME);
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
}
