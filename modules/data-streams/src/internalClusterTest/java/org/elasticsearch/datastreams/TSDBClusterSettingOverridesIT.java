/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TSDBClusterSettingOverridesIT extends ESSingleNodeTestCase {

    private static final String MAPPING_TEMPLATE = """
        {
          "_doc":{
            "properties": {
              "@timestamp" : {
                "type": "date"
              },
              "metricset": {
                "type": "keyword",
                "time_series_dimension": true
              }
            }
          }
        }""";

    private static final String DOC = """
        {
            "@timestamp": "$time",
            "metricset": "pod"
        }
        """;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DataStreamsPlugin.class, InternalSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL.getKey(), "10m")
            .put(DataStreamIndexSettingsProvider.SUPPORT_SYNTHETIC_ID.getKey(), false)
            .put(DataStreamIndexSettingsProvider.SUPPORT_SEQ_NO_DISABLED.getKey(), false)
            .build();
    }

    public void testClusterSettingOverrideForSeqNoAndSyntheticIDs() throws Exception {
        createTsdbDataStream("k8s-synthetic-id");

        Settings indexSettings = getBackingIndexSettings("k8s-synthetic-id");
        assertThat(IndexSettings.SYNTHETIC_ID.get(indexSettings), equalTo(false));
        assertThat(IndexSettings.DISABLE_SEQUENCE_NUMBERS.get(indexSettings), equalTo(false));
    }

    private void createTsdbDataStream(String dataStreamName) throws Exception {
        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("id-" + dataStreamName);
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName + "*"))
                .template(
                    new Template(
                        Settings.builder().put("index.mode", "time_series").build(),
                        new CompressedXContent(MAPPING_TEMPLATE),
                        null
                    )
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet();

        var indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
        indexRequest.source(DOC.replace("$time", TSDBIndexingIT.formatInstant(Instant.now())), XContentType.JSON);
        client().index(indexRequest).actionGet();
    }

    private Settings getBackingIndexSettings(String dataStreamName) {
        GetIndexResponse getIndexResponse = safeGet(
            indicesAdmin().getIndex(new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices(dataStreamName))
        );
        assertThat(getIndexResponse.getIndices().length, equalTo(1));
        return getIndexResponse.getSettings().get(getIndexResponse.getIndices()[0]);
    }
}
