/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamMappingsAction;
import org.elasticsearch.action.datastreams.UpdateDataStreamMappingsAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class TransportUpdateDataStreamMappingsActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    public void testGetAndUpdateMappings() throws IOException {
        String dataStreamName = "my-data-stream-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        createDataStream(dataStreamName);

        Map<String, Object> originalMappings = Map.of(
            "dynamic",
            "strict",
            "properties",
            Map.of("foo1", Map.of("type", "text"), "foo2", Map.of("type", "text"))
        );
        Map<String, Object> mappingOverrides = Map.of(
            "properties",
            Map.of("foo2", Map.of("type", "keyword"), "foo3", Map.of("type", "text"))
        );
        Map<String, Object> expectedEffectiveMappings = Map.of(
            "dynamic",
            "strict",
            "properties",
            Map.of("foo1", Map.of("type", "text"), "foo2", Map.of("type", "keyword"), "foo3", Map.of("type", "text"))
        );
        assertExpectedMappings(dataStreamName, Map.of(), originalMappings);
        updateMappings(dataStreamName, mappingOverrides, expectedEffectiveMappings, true);
        assertExpectedMappings(dataStreamName, Map.of(), originalMappings);
        updateMappings(dataStreamName, mappingOverrides, expectedEffectiveMappings, false);
        assertExpectedMappings(dataStreamName, mappingOverrides, expectedEffectiveMappings);

        // Now make sure that the backing index still has the original mappings:
        Map<String, Object> originalIndexMappings = Map.of(
            "dynamic",
            "strict",
            "_data_stream_timestamp",
            Map.of("enabled", true),
            "properties",
            Map.of("@timestamp", Map.of("type", "date"), "foo1", Map.of("type", "text"), "foo2", Map.of("type", "text"))
        );
        assertExpectedMappingsOnIndex(getDataStream(dataStreamName).getIndices().getFirst().getName(), originalIndexMappings);

        // Do a rollover, and then make sure that the updated mappnigs are on the new write index:
        assertAcked(indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet());
        Map<String, Object> updatedIndexMappings = Map.of(
            "dynamic",
            "strict",
            "_data_stream_timestamp",
            Map.of("enabled", true),
            "properties",
            Map.of(
                "@timestamp",
                Map.of("type", "date"),
                "foo1",
                Map.of("type", "text"),
                "foo2",
                Map.of("type", "keyword"),
                "foo3",
                Map.of("type", "text")
            )
        );
        assertExpectedMappingsOnIndex(getDataStream(dataStreamName).getIndices().get(1).getName(), updatedIndexMappings);

        // Now undo the mapping overrides, and expect the original mapping to be in effect:
        updateMappings(dataStreamName, Map.of(), originalMappings, false);
        assertExpectedMappings(dataStreamName, Map.of(), originalMappings);
        assertAcked(indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet());
        assertExpectedMappingsOnIndex(getDataStream(dataStreamName).getIndices().get(2).getName(), originalIndexMappings);
    }

    private void createDataStream(String dataStreamName) throws IOException {
        String mappingString = """
            {
              "_doc":{
                "dynamic":"strict",
                "properties":{
                  "foo1":{
                    "type":"text"
                  },
                  "foo2":{
                    "type":"text"
                  }
                }
              }
            }
            """;
        CompressedXContent mapping = CompressedXContent.fromJSON(mappingString);
        Template template = new Template(Settings.EMPTY, mapping, null);
        ComposableIndexTemplate.DataStreamTemplate dataStreamTemplate = new ComposableIndexTemplate.DataStreamTemplate();
        ComposableIndexTemplate composableIndexTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("my-data-stream-*"))
            .dataStreamTemplate(dataStreamTemplate)
            .template(template)
            .build();
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request("test");
        request.indexTemplate(composableIndexTemplate);

        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(dataStreamName).source("""
            {
              "@timestamp": "2024-08-27",
              "foo1": "baz"
            }
            """, XContentType.JSON).id(randomUUID()));
        bulkRequest.add(new IndexRequest(dataStreamName).source("""
            {
              "@timestamp": "2024-08-27",
              "foo3": "baz"
            }
            """, XContentType.JSON).id(randomUUID()));
        BulkResponse response = client().execute(new ActionType<BulkResponse>(TransportBulkAction.NAME), bulkRequest).actionGet();
        assertThat(response.getItems().length, equalTo(2));
    }

    private void assertExpectedMappings(
        String dataStreamName,
        Map<String, Object> expectedMappingOverrides,
        Map<String, Object> expectedEffectiveMappings
    ) {
        GetDataStreamMappingsAction.Response getMappingsResponse = client().execute(
            new ActionType<GetDataStreamMappingsAction.Response>(GetDataStreamMappingsAction.NAME),
            new GetDataStreamMappingsAction.Request(TimeValue.THIRTY_SECONDS).indices(dataStreamName)
        ).actionGet();
        List<GetDataStreamMappingsAction.DataStreamMappingsResponse> responses = getMappingsResponse.getDataStreamMappingsResponses();
        assertThat(responses.size(), equalTo(1));
        GetDataStreamMappingsAction.DataStreamMappingsResponse mappingsResponse = responses.getFirst();
        assertThat(mappingsResponse.dataStreamName(), equalTo(dataStreamName));
        assertThat(
            XContentHelper.convertToMap(mappingsResponse.mappings().uncompressed(), true, XContentType.JSON).v2(),
            equalTo(expectedMappingOverrides)
        );
        assertThat(
            XContentHelper.convertToMap(mappingsResponse.effectiveMappings().uncompressed(), true, XContentType.JSON).v2(),
            equalTo(expectedEffectiveMappings)
        );

        DataStream dataStream = getDataStream(dataStreamName);
        assertThat(
            XContentHelper.convertToMap(dataStream.getMappings().uncompressed(), true, XContentType.JSON).v2(),
            equalTo(expectedMappingOverrides)
        );
    }

    private void assertExpectedMappingsOnIndex(String indexName, Map<String, Object> expectedMappings) {
        GetMappingsResponse mappingsResponse = client().execute(
            new ActionType<GetMappingsResponse>(GetMappingsAction.NAME),
            new GetMappingsRequest(TimeValue.THIRTY_SECONDS).indices(indexName)
        ).actionGet();
        Map<String, MappingMetadata> mappings = mappingsResponse.mappings();
        assertThat(mappings.size(), equalTo(1));
        assertThat(mappings.values().iterator().next().sourceAsMap(), equalTo(expectedMappings));
    }

    private void updateMappings(
        String dataStreamName,
        Map<String, Object> mappingOverrides,
        Map<String, Object> expectedEffectiveMappings,
        boolean dryRun
    ) throws IOException {
        CompressedXContent mappingOverride = new CompressedXContent(mappingOverrides);
        UpdateDataStreamMappingsAction.Response putMappingsResponse = client().execute(
            new ActionType<UpdateDataStreamMappingsAction.Response>(UpdateDataStreamMappingsAction.NAME),
            new UpdateDataStreamMappingsAction.Request(mappingOverride, dryRun, TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).indices(
                dataStreamName
            )
        ).actionGet();
        assertThat(putMappingsResponse.getDataStreamMappingsResponses().size(), equalTo(1));
        UpdateDataStreamMappingsAction.DataStreamMappingsResponse firstPutMappingsResponse = putMappingsResponse
            .getDataStreamMappingsResponses()
            .getFirst();
        assertThat(firstPutMappingsResponse.dataStreamName(), equalTo(dataStreamName));
        assertThat(
            XContentHelper.convertToMap(firstPutMappingsResponse.mappings().uncompressed(), true, XContentType.JSON).v2(),
            equalTo(mappingOverrides)
        );
        assertThat(
            XContentHelper.convertToMap(firstPutMappingsResponse.effectiveMappings().uncompressed(), true, XContentType.JSON).v2(),
            equalTo(expectedEffectiveMappings)
        );
    }

    private DataStream getDataStream(String dataStreamName) {
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            new String[] { dataStreamName }
        );
        GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        return getDataStreamResponse.getDataStreams().get(0).getDataStream();
    }
}
