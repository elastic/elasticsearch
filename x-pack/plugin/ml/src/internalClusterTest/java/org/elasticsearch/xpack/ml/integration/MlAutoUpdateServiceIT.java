/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.Version;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.MlAutoUpdateService;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfigAutoUpdater;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.junit.Before;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.nullValue;

public class MlAutoUpdateServiceIT extends MlSingleNodeTestCase {

    private DatafeedConfigProvider datafeedConfigProvider;

    @Before
    public void createComponents() throws Exception {
        datafeedConfigProvider = new DatafeedConfigProvider(client(), xContentRegistry(), getInstanceFromNode(ClusterService.class));
        waitForMlTemplates();
    }

    private static final String AGG_WITH_OLD_DATE_HISTOGRAM_INTERVAL = """
        {
            "datafeed_id": "farequote-datafeed-with-old-agg",
            "job_id": "farequote",
            "frequency": "1h",
            "config_type": "datafeed",
            "indices": ["farequote1", "farequote2"],
            "aggregations": {
            "buckets": {
              "date_histogram": {
                "field": "time",
                "interval": "360s",
                "time_zone": "UTC"
              },
              "aggregations": {
                "time": {
                  "max": {"field": "time"}
                }
              }
            }
          }
        }""";

    public void testAutomaticModelUpdate() throws Exception {
        ensureGreen("_all");
        IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();
        client().prepareIndex(MlConfigIndex.indexName())
            .setId(DatafeedConfig.documentId("farequote-datafeed-with-old-agg"))
            .setSource(AGG_WITH_OLD_DATE_HISTOGRAM_INTERVAL, XContentType.JSON)
            .get();
        AtomicReference<DatafeedConfig.Builder> getConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(
            listener -> datafeedConfigProvider.getDatafeedConfig("farequote-datafeed-with-old-agg", null, listener),
            getConfigHolder,
            exceptionHolder
        );
        assertThat(exceptionHolder.get(), is(nullValue()));
        client().admin().indices().prepareRefresh(MlConfigIndex.indexName()).get();

        DatafeedConfigAutoUpdater autoUpdater = new DatafeedConfigAutoUpdater(datafeedConfigProvider, indexNameExpressionResolver);
        MlAutoUpdateService mlAutoUpdateService = new MlAutoUpdateService(client().threadPool(), Collections.singletonList(autoUpdater));

        ClusterChangedEvent event = new ClusterChangedEvent(
            "test",
            ClusterState.builder(new ClusterName("test"))
                .nodes(
                    DiscoveryNodes.builder()
                        .add(
                            new DiscoveryNode(
                                "node_name",
                                "node_id",
                                new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                                Collections.emptyMap(),
                                Set.of(DiscoveryNodeRole.MASTER_ROLE),
                                Version.V_8_0_0
                            )
                        )
                        .localNodeId("node_id")
                        .masterNodeId("node_id")
                        .build()
                )
                .build(),
            ClusterState.builder(new ClusterName("test")).build()
        );

        mlAutoUpdateService.clusterChanged(event);
        assertBusy(() -> {
            try {
                GetResponse getResponse = client().prepareGet(
                    MlConfigIndex.indexName(),
                    DatafeedConfig.documentId("farequote-datafeed-with-old-agg")
                ).get();
                assertTrue(getResponse.isExists());
                assertThat(getResponse.getSourceAsString(), containsString("fixed_interval"));
            } catch (Exception ex) {
                fail(ex.getMessage());
            }
        });
    }

}
