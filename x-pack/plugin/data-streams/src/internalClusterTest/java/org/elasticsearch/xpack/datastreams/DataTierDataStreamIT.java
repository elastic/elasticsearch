/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.datastreams;

import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class DataTierDataStreamIT extends ESIntegTestCase {
    private static final String index = "myindex";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, DataStreamsPlugin.class);
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
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(idxSettings), equalTo(DataTier.DATA_HOT));

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
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(idxSettings), equalTo(DataTier.DATA_HOT));

        client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { index }));
    }

    public void startHotOnlyNode() {
        Settings nodeSettings = Settings.builder().putList("node.roles", Arrays.asList("master", "data_hot", "ingest")).build();
        internalCluster().startNode(nodeSettings);
    }
}
