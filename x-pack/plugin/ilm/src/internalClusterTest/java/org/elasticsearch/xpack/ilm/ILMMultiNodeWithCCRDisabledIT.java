/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleResponse;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleExplainResponse;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.ilm.ShrinkIndexNameSupplier.SHRUNKEN_INDEX_PREFIX;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ILMMultiNodeWithCCRDisabledIT extends ESIntegTestCase {
    private static final String index = "myindex";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, DataStreamsPlugin.class, IndexLifecycle.class, Ccr.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s")
            // This just generates less churn and makes it easier to read the log file if needed
            .put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false)
            .put(XPackSettings.CCR_ENABLED_SETTING.getKey(), false)
            .build();
    }

    public void testShrinkOnTiers() throws Exception {
        startHotOnlyNode();
        startWarmOnlyNode();
        ensureGreen();
        Map<String, LifecycleAction> actions = new HashMap<>();
        RolloverAction rolloverAction = new RolloverAction(null, null, null, 1L, null, null, null, null, null, null);
        ShrinkAction shrinkAction = new ShrinkAction(1, null);
        actions.put(rolloverAction.getWriteableName(), rolloverAction);
        actions.put(shrinkAction.getWriteableName(), shrinkAction);
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, actions);

        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy("shrink-policy", Collections.singletonMap(hotPhase.getName(), hotPhase));
        client().execute(PutLifecycleAction.INSTANCE, new PutLifecycleAction.Request(lifecyclePolicy)).get();

        Template t = new Template(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, "shrink-policy")
                .build(),
            null,
            null
        );

        ComposableIndexTemplate template = new ComposableIndexTemplate(
            Collections.singletonList(index),
            t,
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
        client().prepareIndex(index).setCreate(true).setId("1").setSource("@timestamp", "2020-09-09").get();

        assertBusy(() -> {
            ExplainLifecycleResponse explain = client().execute(ExplainLifecycleAction.INSTANCE, new ExplainLifecycleRequest().indices("*"))
                .get();
            logger.info("--> explain: {}", Strings.toString(explain));

            String backingIndexName = DataStream.getDefaultBackingIndexName(index, 1);
            IndexLifecycleExplainResponse indexResp = null;
            for (Map.Entry<String, IndexLifecycleExplainResponse> indexNameAndResp : explain.getIndexResponses().entrySet()) {
                if (indexNameAndResp.getKey().startsWith(SHRUNKEN_INDEX_PREFIX) && indexNameAndResp.getKey().contains(backingIndexName)) {
                    indexResp = indexNameAndResp.getValue();
                    assertNotNull(indexResp);
                    assertThat(indexResp.getPhase(), equalTo("hot"));
                    assertThat(indexResp.getStep(), equalTo("complete"));
                    break;
                }
            }

            assertNotNull("Unable to find an ilm explain output for the shrunk index of " + index, indexResp);
        }, 30, TimeUnit.SECONDS);
    }

    public void startHotOnlyNode() {
        Settings nodeSettings = Settings.builder().putList("node.roles", Arrays.asList("master", "data_hot", "ingest")).build();
        internalCluster().startNode(nodeSettings);
    }

    public void startWarmOnlyNode() {
        Settings nodeSettings = Settings.builder().putList("node.roles", Arrays.asList("master", "data_warm", "ingest")).build();
        internalCluster().startNode(nodeSettings);
    }
}
