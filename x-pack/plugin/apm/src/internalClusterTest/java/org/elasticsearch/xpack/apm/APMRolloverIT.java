/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
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
import org.elasticsearch.xpack.stack.StackPlugin;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasSize;

@ESTestCase.WithoutSecurityManager
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class APMRolloverIT extends ESIntegTestCase {

    private APMIndexTemplateRegistry registry;

    @Before
    public void obtainRegistry() {
        registry = internalCluster().getInstance(APMIndexTemplateRegistry.class, internalCluster().getMasterName());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            APMPlugin.class,
            StackPlugin.class,
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
        // todo see org.elasticsearch.datastreams.DataStreamUpgradeRestIT.waitForLogsComponentTemplateInitialization
        Thread.sleep(30000);
        Map<String, ComposableIndexTemplate> composableTemplateConfigs = registry.getComposableTemplateConfigs();
        composableTemplateConfigs.values().forEach(indexTemplate -> {
            String indexPattern = indexTemplate.indexPatterns().get(0);
            String dsName = indexPattern.replace("*", "test");
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dsName);
            try {
                AcknowledgedResponse acknowledgedResponse = client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest)
                    .get();
                System.out.println("acknowledgedResponse = " + acknowledgedResponse);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("metrics-foo");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[]{"*"});
        GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        List<GetDataStreamAction.Response.DataStreamInfo> dataStreams = getDataStreamResponse.getDataStreams();
        dataStreams.forEach(ds -> assertThat(ds.getDataStream().getIndices(), hasSize(1)));

        registry.setVersion(registry.getVersion() + 1);
        assertAcked(prepareCreate("test1"));

        // todo: now use assertBusy to wait until each data stream has two backing indices (indicating rollover has occurred)
    }
}
