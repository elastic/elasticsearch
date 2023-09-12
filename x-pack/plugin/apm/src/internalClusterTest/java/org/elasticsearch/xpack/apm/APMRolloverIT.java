/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin;
import org.elasticsearch.ingest.useragent.IngestUserAgentPlugin;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.ilm.history.ILMHistoryTemplateRegistry;
import org.elasticsearch.xpack.stack.StackPlugin;
import org.elasticsearch.xpack.stack.StackTemplateRegistry;
import org.elasticsearch.xpack.wildcard.Wildcard;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;

@SuppressWarnings("resource")
// the Painless compiler requires the "createClassLoader" permission
@ESTestCase.WithoutSecurityManager
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class APMRolloverIT extends ESIntegTestCase {

    private ClusterService clusterService;
    private APMIndexTemplateRegistry apmIndexTemplateRegistry;
    private StackTemplateRegistry stackTemplateRegistry;
    private ILMHistoryTemplateRegistry ilmHistoryTemplateRegistry;

    @Before
    public void obtainRegistry() {
        clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        apmIndexTemplateRegistry = internalCluster().getInstance(APMIndexTemplateRegistry.class, internalCluster().getMasterName());
        stackTemplateRegistry = internalCluster().getInstance(StackTemplateRegistry.class, internalCluster().getMasterName());
        ilmHistoryTemplateRegistry = internalCluster().getInstance(ILMHistoryTemplateRegistry.class, internalCluster().getMasterName());
    }

    @After
    public void unregisterRegistries() {
        // we must remove the registries from the cluster state listeners' list, otherwise they mess up the cluster cleanup assertions by
        // automatically reinstalling resources
        clusterService.removeListener(apmIndexTemplateRegistry);
        clusterService.removeListener(stackTemplateRegistry);
        clusterService.removeListener(ilmHistoryTemplateRegistry);
        // todo: find a way to cancel the geoip database download task as it throws errors when terminating the test
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            APMPlugin.class,
            StackPlugin.class,
            IndexLifecycle.class,
            Wildcard.class,
            DataStreamsPlugin.class,
            XPackPlugin.class,
            IngestCommonPlugin.class,
            IngestGeoIpPlugin.class,
            IngestUserAgentPlugin.class,
            PainlessPlugin.class,
            MapperExtrasPlugin.class,
            ConstantKeywordMapperPlugin.class,
            AggregateMetricMapperPlugin.class,
            AnalyticsPlugin.class
        );
    }

    public void testRollover() throws Exception {
        Set<String> apmIndexTemplates = new HashSet<>(apmIndexTemplateRegistry.getComposableTemplateConfigs().keySet());
        ClusterState state = clusterService.state();
        apmIndexTemplateRegistry.clusterChanged(new ClusterChangedEvent(APMRolloverIT.class.getName(), state, state));
        // No need to waste the initial frequent assertBusy iterations because this takes a few seconds at least
        Thread.sleep(3000);
        assertBusy(() -> {
            // todo - figure out why we need this synthetic cluster change event trigger to make sure the last index template is registered
            // maybe because org.elasticsearch.ingest.IngestService.putPipeline() doesn't update the cluster state and trigger state
            // change event like, for example, org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.addComponentTemplate does
            apmIndexTemplateRegistry.clusterChanged(
                new ClusterChangedEvent(APMRolloverIT.class.getName(), clusterService.state(), clusterService.state())
            );
            apmIndexTemplates.removeIf(apmIndexTemplate -> clusterService.state().metadata().templatesV2().containsKey(apmIndexTemplate));
            assertTrue(apmIndexTemplates.isEmpty());
        });
        Map<String, ComposableIndexTemplate> composableTemplateConfigs = apmIndexTemplateRegistry.getComposableTemplateConfigs();
        List<ActionFuture<AcknowledgedResponse>> responseFutures = composableTemplateConfigs.values().stream().map(indexTemplate -> {
            String indexPattern = indexTemplate.indexPatterns().get(0);
            String dsName = indexPattern.replace("*", "test");
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dsName);
            return client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest);
        }).toList();
        responseFutures.forEach(responseFuture -> {
            try {
                assertTrue(responseFuture.get().isAcknowledged());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertNumberOfBackingIndices(1);

        state = clusterService.state();
        final Map<String, ComposableIndexTemplate> indexTemplatesWithLowerVersion = new HashMap<>();
        state.metadata()
            .templatesV2()
            .forEach(
                (name, template) -> indexTemplatesWithLowerVersion.put(
                    name,
                    new ComposableIndexTemplate(
                        template.indexPatterns(),
                        template.template(),
                        template.composedOf(),
                        template.priority(),
                        template.version() - 1,
                        template.metadata()
                    )
                )
            );
        ClusterState stateWithLowerVersion = state.copyAndUpdateMetadata(builder -> builder.indexTemplates(indexTemplatesWithLowerVersion));
        apmIndexTemplateRegistry.clusterChanged(new ClusterChangedEvent(APMRolloverIT.class.getName(), stateWithLowerVersion, state));

        assertBusy(() -> assertNumberOfBackingIndices(2));
    }

    private static void assertNumberOfBackingIndices(final int expected) {
        final GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { "*" });
        final GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        final List<GetDataStreamAction.Response.DataStreamInfo> dataStreams = getDataStreamResponse.getDataStreams();
        dataStreams.forEach(ds -> {
            DataStream dataStream = ds.getDataStream();
            assertThat(dataStream.getIndices(), hasSize(expected));
            assertThat(dataStream.getWriteIndex().getName(), endsWith(String.valueOf(expected)));
        });
    }
}
