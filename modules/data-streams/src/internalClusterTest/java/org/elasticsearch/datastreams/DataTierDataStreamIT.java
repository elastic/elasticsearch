/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
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

        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(Collections.singletonList(index))

            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();
        client().execute(
            TransportPutComposableIndexTemplateAction.TYPE,
            new TransportPutComposableIndexTemplateAction.Request("template").indexTemplate(template)
        ).actionGet();

        var dsIndexName = prepareIndex(index).setCreate(true)
            .setId("1")
            .setSource("@timestamp", "2020-09-09")
            .setWaitForActiveShards(0)
            .get()
            .getIndex();
        var idxSettings = indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices(index).get().getSettings().get(dsIndexName);
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(idxSettings), equalTo(DataTier.DATA_HOT));

        logger.info("--> waiting for {} to be yellow", index);
        ensureYellow(index);

        // Roll over index and ensure the second index also went to the "hot" tier
        var rolledOverIndexName = indicesAdmin().prepareRolloverIndex(index).get().getNewIndex();

        // new index name should have the rolled over name
        assertNotEquals(dsIndexName, rolledOverIndexName);

        idxSettings = indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices(index).get().getSettings().get(rolledOverIndexName);
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(idxSettings), equalTo(DataTier.DATA_HOT));
    }

    public void startHotOnlyNode() {
        Settings nodeSettings = Settings.builder().putList("node.roles", Arrays.asList("master", "data_hot", "ingest")).build();
        internalCluster().startNode(nodeSettings);
    }
}
