/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.sampling.GetSampleAction;
import org.elasticsearch.action.admin.indices.sampling.PutSampleConfigurationAction;
import org.elasticsearch.action.admin.indices.sampling.SamplingConfiguration;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.SamplingService.RANDOM_SAMPLING_FEATURE_FLAG;
import static org.elasticsearch.ingest.SamplingService.TTL_POLL_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class SamplingServiceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    public void testTTL() throws Exception {
        assumeTrue("Requires sampling feature flag", RANDOM_SAMPLING_FEATURE_FLAG);
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(Map.of(TTL_POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1)))
        );
        String indexName = randomIdentifier();
        client().execute(TransportCreateIndexAction.TYPE, new CreateIndexRequest(indexName)).actionGet();
        ensureYellow(indexName);
        PutSampleConfigurationAction.Request putSampleConfigRequest = new PutSampleConfigurationAction.Request(
            new SamplingConfiguration(1.0d, 10, null, TimeValue.timeValueSeconds(1), null),
            TimeValue.THIRTY_SECONDS,
            TimeValue.THIRTY_SECONDS
        ).indices(indexName);
        client().execute(PutSampleConfigurationAction.INSTANCE, putSampleConfigRequest).actionGet();
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 20; i++) {
            IndexRequest indexRequest = new IndexRequest(indexName);
            indexRequest.source(Map.of("foo", randomBoolean() ? 3L : randomLong(), "bar", randomBoolean()));
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = client().execute(TransportBulkAction.TYPE, bulkRequest).actionGet();
        assertThat(bulkResponse.hasFailures(), equalTo(false));
        GetSampleAction.Response getSampleResponse = client().execute(GetSampleAction.INSTANCE, new GetSampleAction.Request(indexName))
            .actionGet();
        assertThat(getSampleResponse.getSample().size(), equalTo(10));
        assertBusy(() -> {
            assertThrows(
                ResourceNotFoundException.class,
                () -> client().execute(GetSampleAction.INSTANCE, new GetSampleAction.Request(indexName)).actionGet()
            );
        });
    }

    public void testDeleteIndex() throws Exception {
        assumeTrue("Requires sampling feature flag", RANDOM_SAMPLING_FEATURE_FLAG);
        String indexName = randomIdentifier();
        client().execute(TransportCreateIndexAction.TYPE, new CreateIndexRequest(indexName)).actionGet();
        ensureYellow(indexName);
        PutSampleConfigurationAction.Request putSampleConfigRequest = new PutSampleConfigurationAction.Request(
            new SamplingConfiguration(1.0d, 10, null, null, null),
            TimeValue.THIRTY_SECONDS,
            TimeValue.THIRTY_SECONDS
        ).indices(indexName);
        client().execute(PutSampleConfigurationAction.INSTANCE, putSampleConfigRequest).actionGet();
        for (int i = 0; i < 5; i++) {
            BulkRequest bulkRequest = new BulkRequest();
            for (int j = 0; j < 20; j++) {
                IndexRequest indexRequest = new IndexRequest(indexName);
                indexRequest.source(Map.of("foo", randomBoolean() ? 3L : randomLong(), "bar", randomBoolean()));
                bulkRequest.add(indexRequest);
            }
            BulkResponse bulkResponse = client().execute(TransportBulkAction.TYPE, bulkRequest).actionGet();
            assertThat(bulkResponse.hasFailures(), equalTo(false));
        }
        GetSampleAction.Response getSampleResponse = client().execute(GetSampleAction.INSTANCE, new GetSampleAction.Request(indexName))
            .actionGet();
        assertThat(getSampleResponse.getSample().size(), equalTo(10));
        client().execute(TransportDeleteIndexAction.TYPE, new DeleteIndexRequest(indexName)).actionGet();
        assertBusy(() -> {
            for (SamplingService samplingService : internalCluster().getInstances(SamplingService.class)) {
                assertThat(samplingService.getLocalSample(ProjectId.DEFAULT, indexName), equalTo(List.of()));
            }
        });
    }

    public void testDeleteDataStream() throws Exception {
        assumeTrue("Requires sampling feature flag", RANDOM_SAMPLING_FEATURE_FLAG);
        String indexName = randomIdentifier();
        var template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(indexName))

            .template(new Template(Settings.EMPTY, CompressedXContent.fromJSON("""
                {
                  "_doc":{
                    "dynamic":true,
                    "properties":{
                      "foo":{
                        "type":"text"
                      },
                      "bar":{
                        "type":"text"
                      }
                    }
                  }
                }
                """), null))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
            .build();
        var request = new TransportPutComposableIndexTemplateAction.Request("logs-template");
        request.indexTemplate(template);
        safeGet(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));
        client().execute(
            CreateDataStreamAction.INSTANCE,
            new CreateDataStreamAction.Request(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS, indexName)
        ).actionGet();
        ensureYellow(indexName);
        PutSampleConfigurationAction.Request putSampleConfigRequest = new PutSampleConfigurationAction.Request(
            new SamplingConfiguration(1.0d, 10, null, null, null),
            TimeValue.THIRTY_SECONDS,
            TimeValue.THIRTY_SECONDS
        ).indices(indexName);
        client().execute(PutSampleConfigurationAction.INSTANCE, putSampleConfigRequest).actionGet();
        for (int i = 0; i < 5; i++) {
            BulkRequest bulkRequest = new BulkRequest();
            for (int j = 0; j < 20; j++) {
                IndexRequest indexRequest = new IndexRequest(indexName);
                indexRequest.create(true);
                indexRequest.source(Map.of("@timestamp", 12345, "foo", randomBoolean() ? 3L : randomLong(), "bar", randomBoolean()));
                bulkRequest.add(indexRequest);
            }
            BulkResponse bulkResponse = client().execute(TransportBulkAction.TYPE, bulkRequest).actionGet();
            assertThat(bulkResponse.hasFailures(), equalTo(false));
        }
        GetSampleAction.Response getSampleResponse = client().execute(GetSampleAction.INSTANCE, new GetSampleAction.Request(indexName))
            .actionGet();
        assertThat(getSampleResponse.getSample().size(), equalTo(10));
        client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(TimeValue.THIRTY_SECONDS, indexName))
            .actionGet();
        assertBusy(() -> {
            for (SamplingService samplingService : internalCluster().getInstances(SamplingService.class)) {
                assertThat(samplingService.getLocalSample(ProjectId.DEFAULT, indexName), equalTo(List.of()));
            }
        });
    }

    @After
    public void cleanup() {
        Map<String, Object> clearedSettings = new HashMap<>();
        clearedSettings.put(TTL_POLL_INTERVAL_SETTING.getKey(), null);
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setPersistentSettings(clearedSettings)
        );
    }
}
