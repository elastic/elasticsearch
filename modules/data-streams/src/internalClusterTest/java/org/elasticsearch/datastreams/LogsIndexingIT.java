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
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class LogsIndexingIT extends ESSingleNodeTestCase {

    public static final String MAPPING_TEMPLATE = """
        {
          "_doc":{
            "properties": {
              "@timestamp" : {
                "type": "date"
              },
              "metricset": {
                "type": "keyword"
              }
            }
          }
        }""";

    private static final String DOC = """
        {
            "@timestamp": "$time",
            "metricset": "pod",
            "k8s": {
                "pod": {
                    "name": "dog",
                    "uid":"$uuid",
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
        return List.of(DataStreamsPlugin.class, InternalSettingsPlugin.class);
    }

    public void testIndexSearchAndRetrieval() throws Exception {
        String[] uuis = {
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString() };

        String dataStreamName = "k8s";
        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("id");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName + "*"))
                .template(
                    new Template(
                        Settings.builder()
                            .put("index.mode", "logsdb")
                            .put("index.routing_path", "metricset,k8s.pod.uid")
                            .put("index.number_of_replicas", 0)
                            // Reduce sync interval to speedup this integraton test,
                            // otherwise by default it will take 30 seconds before minimum retained seqno is updated:
                            .put("index.soft_deletes.retention_lease.sync_interval", "100ms")
                            .build(),
                        new CompressedXContent(MAPPING_TEMPLATE),
                        null
                    )
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet();

        // index some data
        int numBulkRequests = randomIntBetween(128, 1024);
        ;
        int numDocsPerBulk = randomIntBetween(16, 256);
        String indexName = null;
        {
            Instant time = Instant.now();
            for (int i = 0; i < numBulkRequests; i++) {
                BulkRequest bulkRequest = new BulkRequest(dataStreamName);
                for (int j = 0; j < numDocsPerBulk; j++) {
                    var indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
                    indexRequest.source(
                        DOC.replace("$time", formatInstant(time)).replace("$uuid", uuis[j % uuis.length]),
                        XContentType.JSON
                    );
                    bulkRequest.add(indexRequest);
                    time = time.plusMillis(1);
                }
                var bulkResponse = client().bulk(bulkRequest).actionGet();
                assertThat(bulkResponse.hasFailures(), is(false));
                indexName = bulkResponse.getItems()[0].getIndex();
            }
            client().admin().indices().refresh(new RefreshRequest(dataStreamName)).actionGet();
        }

        // Check the search api can synthesize _id
        final String idxName = indexName;
        var searchRequest = new SearchRequest(dataStreamName);
        searchRequest.source().trackTotalHits(true);
        assertResponse(client().search(searchRequest), searchResponse -> {
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numBulkRequests * numDocsPerBulk));
            String id = searchResponse.getHits().getHits()[0].getId();
            assertThat(id, notNullValue());

            // Check that the _id is gettable:
            var getResponse = client().get(new GetRequest(idxName).id(id)).actionGet();
            assertThat(getResponse.isExists(), is(true));
            assertThat(getResponse.getId(), equalTo(id));
        });
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

}
