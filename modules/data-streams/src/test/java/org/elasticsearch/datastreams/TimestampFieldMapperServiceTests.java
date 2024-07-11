/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TimestampFieldMapperServiceTests extends ESSingleNodeTestCase {

    private static final String DOC = """
        {
            "@timestamp": "$time",
            "metricset": "pod",
            "k8s": {
                "pod": {
                    "name": "dog",
                    "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea9",
                    "ip": "10.10.55.3",
                    "network": {
                        "tx": 1434595272,
                        "rx": 530605511
                    }
                }
            }
        }
        """;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    public void testGetTimestampFieldTypeForTsdbDataStream() throws IOException {
        createTemplate(true);
        DocWriteResponse indexResponse = indexDoc();

        var indicesService = getInstanceFromNode(IndicesService.class);
        var result = indicesService.getTimestampFieldTypeInfo(indexResponse.getShardId().getIndex());
        assertThat(result, notNullValue());
    }

    public void testGetTimestampFieldTypeForDataStream() throws IOException {
        createTemplate(false);
        DocWriteResponse indexResponse = indexDoc();

        var indicesService = getInstanceFromNode(IndicesService.class);
        var result = indicesService.getTimestampFieldTypeInfo(indexResponse.getShardId().getIndex());
        assertThat(result, nullValue());
    }

    private DocWriteResponse indexDoc() {
        Instant time = Instant.now();
        var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
        indexRequest.source(DOC.replace("$time", formatInstant(time)), XContentType.JSON);
        return client().index(indexRequest).actionGet();
    }

    private void createTemplate(boolean tsdb) throws IOException {
        var mappingTemplate = """
            {
              "_doc":{
                "properties": {
                  "metricset": {
                    "type": "keyword",
                    "time_series_dimension": true
                  }
                }
              }
            }""";
        var templateSettings = Settings.builder().put("index.mode", tsdb ? "time_series" : "standard");
        var request = new TransportPutComposableIndexTemplateAction.Request("id");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("k8s*"))
                .template(new Template(templateSettings.build(), new CompressedXContent(mappingTemplate), null))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
    }

    private static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

}
