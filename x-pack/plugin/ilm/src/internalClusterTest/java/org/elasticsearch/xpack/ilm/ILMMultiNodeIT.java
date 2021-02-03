/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleResponse;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleExplainResponse;
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

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ILMMultiNodeIT extends ESIntegTestCase {
    private static final String index = "myindex";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");

        // This is necessary to prevent ILM and SLM installing a lifecycle policy, these tests assume a blank slate
        settings.put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false);
        settings.put(LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), false);
        return settings.build();
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Settings transportClientSettings() {
        Settings.Builder settings = Settings.builder().put(super.transportClientSettings());
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        return settings.build();
    }

    public void testShrinkOnTiers() throws Exception {
        startHotOnlyNode();
        startWarmOnlyNode();
        ensureGreen();

        RolloverAction rolloverAction = new RolloverAction(null, null, 1L);
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, Collections.singletonMap(rolloverAction.getWriteableName(), rolloverAction));
        ShrinkAction shrinkAction = new ShrinkAction(1);
        Phase warmPhase = new Phase("warm", TimeValue.ZERO, Collections.singletonMap(shrinkAction.getWriteableName(), shrinkAction));
        Map<String, Phase> phases = new HashMap<>();
        phases.put(hotPhase.getName(), hotPhase);
        phases.put(warmPhase.getName(), warmPhase);
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy("shrink-policy", phases);
        client().execute(PutLifecycleAction.INSTANCE, new PutLifecycleAction.Request(lifecyclePolicy)).get();

        Template t = new Template(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, "shrink-policy")
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "shrink-alias")
            .put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, "data_hot")
            .build(), null, null);

        ComposableIndexTemplate template = new ComposableIndexTemplate(
            Collections.singletonList(index + "*"),
            t,
            null,
            null,
            null,
            null
        );
        client().execute(
            PutComposableIndexTemplateAction.INSTANCE,
            new PutComposableIndexTemplateAction.Request("template").indexTemplate(template)
        ).actionGet();
        client().admin().indices().prepareCreate(index + "-000001")
            .addAlias(new Alias("shrink-alias").writeIndex(true)).get();
        client().prepareIndex(index + "-000001", MapperService.SINGLE_MAPPING_NAME)
            .setCreate(true).setId("1").setSource("@timestamp", "2020-09-09").get();

        assertBusy(() -> {
            String name = "shrink-" + index + "-000001";
            ExplainLifecycleResponse explain =
                client().execute(ExplainLifecycleAction.INSTANCE, new ExplainLifecycleRequest().indices("*")).get();
            logger.info("--> explain: {}", Strings.toString(explain));

            IndexLifecycleExplainResponse indexResp = explain.getIndexResponses().get(name);
            assertNotNull(indexResp);
            assertThat(indexResp.getPhase(), equalTo("warm"));
            assertThat(indexResp.getStep(), equalTo("complete"));
        }, 60, TimeUnit.SECONDS);
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
