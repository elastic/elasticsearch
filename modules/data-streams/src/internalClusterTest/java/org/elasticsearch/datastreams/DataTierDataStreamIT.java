/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class DataTierDataStreamIT extends ESIntegTestCase {
    private static final String index = "myindex";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(DataStreamsPlugin.class);
    }

    public void testDefaultDataStreamAllocateToHot() {
        startHotOnlyNode();
        ensureGreen();

        ComposableIndexTemplate template = new ComposableIndexTemplate(
            Collections.singletonList(index),
            null,
            null,
            null,
            null,
            null,
            new ComposableIndexTemplate.DataStreamTemplate(),
            null
        );
        client().execute(
            PutComposableIndexTemplateAction.INSTANCE,
            new PutComposableIndexTemplateAction.Request("template").indexTemplate(template)
        ).actionGet();
        client().prepareIndex(index).setCreate(true).setId("1").setSource("@timestamp", "2020-09-09").setWaitForActiveShards(0).get();

        Settings idxSettings = client().admin()
            .indices()
            .prepareGetIndex()
            .addIndices(index)
            .get()
            .getSettings()
            .get(DataStream.getDefaultBackingIndexName(index, 1));
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(idxSettings), equalTo(DataTier.DATA_HOT));

        logger.info("--> waiting for {} to be yellow", index);
        ensureYellow(index);

        // Roll over index and ensure the second index also went to the "hot" tier
        client().admin().indices().prepareRolloverIndex(index).get();
        idxSettings = client().admin()
            .indices()
            .prepareGetIndex()
            .addIndices(index)
            .get()
            .getSettings()
            .get(DataStream.getDefaultBackingIndexName(index, 2));
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(idxSettings), equalTo(DataTier.DATA_HOT));
    }

    public void startHotOnlyNode() {
        Settings nodeSettings = Settings.builder().putList("node.roles", Arrays.asList("master", "data_hot", "ingest")).build();
        internalCluster().startNode(nodeSettings);
    }
}
