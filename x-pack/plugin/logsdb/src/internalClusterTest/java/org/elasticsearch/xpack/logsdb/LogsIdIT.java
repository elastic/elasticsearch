/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.*;

public class LogsIdIT extends ESSingleNodeTestCase {

    public static final String MAPPING_TEMPLATE = """
        {
          "_doc":{
            "properties": {
              "@timestamp" : {
                "type": "date"
              },
              "message": {
                "type": "keyword"
              },
              "k8s": {
                "properties": {
                  "pod": {
                    "properties": {
                      "uid": {
                        "type": "keyword"
                      }
                    }
                  }
                }
              }
            }
          }
        }""";

    private static final String DOC = """
        {
            "@timestamp": "$time",
            "message": "$pod",
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
        return List.of(InternalSettingsPlugin.class, XPackPlugin.class, LogsDBPlugin.class, DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put("cluster.logsdb.enabled", "true")
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .build();
    }

    public void testGetByGeneratedId() throws Exception {
        String dataStreamName = "k8s";
        createTemplate(dataStreamName);

        Instant time = Instant.now();
        BulkRequest bulkRequest = new BulkRequest(dataStreamName);
        int numDocs = randomIntBetween(16, 256);
        for (int j = 0; j < numDocs; j++) {
            var indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(
                DOC.replace("$time", formatInstant(time)).replace("$uuid", UUID.randomUUID().toString()).replace("$pod", "pod-" + j),
                XContentType.JSON
            );
            bulkRequest.add(indexRequest);
            time = time.plusMillis(1);
        }
        var bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.hasFailures(), is(false));
        var indexName = bulkResponse.getItems()[0].getIndex();
        client().admin().indices().refresh(new RefreshRequest(dataStreamName)).actionGet();

        // Check the search api can synthesize _id
        var searchRequest = new SearchRequest(dataStreamName);
        searchRequest.source().trackTotalHits(true);
        assertResponse(client().search(searchRequest), searchResponse -> {
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));

            for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
                SearchHit hit = searchResponse.getHits().getHits()[i];
                String id = hit.getId();
                assertThat(id, notNullValue());

                // Check that the _id is gettable:
                var getRequest = new GetRequest(indexName).id(id);
                var getResponse = client().get(getRequest).actionGet();
                assertThat(getResponse.isExists(), is(true));
                assertThat(getResponse.getId(), equalTo(id));
            }
        });
    }

    public void testGetByProvidedID() throws Exception {
        var indexName = "test-name";
        createIndex(indexName, Settings.builder().put("index.mode", "logsdb").build());
        Instant time = Instant.now();
        BulkRequest bulkRequest = new BulkRequest(indexName);
        int numDocs = randomIntBetween(16, 256);
        for (int j = 0; j < numDocs; j++) {
            var indexRequest = new IndexRequest(indexName).opType(DocWriteRequest.OpType.INDEX).id("id-" + j);
            indexRequest.source(
                DOC.replace("$time", formatInstant(time)).replace("$uuid", UUID.randomUUID().toString()).replace("$pod", "pod-" + j),
                XContentType.JSON
            );
            bulkRequest.add(indexRequest);
            time = time.plusMillis(1);
        }
        var bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.hasFailures(), is(false));
        client().admin().indices().refresh(new RefreshRequest(indexName)).actionGet();

        if (randomBoolean()) {
            flush(indexName, randomBoolean());
        }

        // Check the search api can synthesize _id
        var searchRequest = new SearchRequest(indexName);
        searchRequest.source().trackTotalHits(true);
        assertResponse(client().search(searchRequest), searchResponse -> {
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));

            for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
                SearchHit hit = searchResponse.getHits().getHits()[i];
                String id = hit.getId();
                String numPart = id.split("-")[1];
                assertThat(id, notNullValue());
                // check got correct doc in search response
                var pod = (String) hit.getSourceAsMap().get("message");
                assertThat(pod, equalTo("pod-" + numPart));
            }
        });

        // test get with the provided IDs
        for (int i = 0; i < numDocs; i++) {
            String id = "id-" + i;
            var getRequest = new GetRequest(indexName).id(id).fetchSourceContext(FetchSourceContext.FETCH_SOURCE);
            var getResponse = client().get(getRequest).actionGet();
            assertThat(getResponse.isExists(), is(true));
            assertThat(getResponse.getId(), equalTo(id));
            assertThat(getResponse.getSourceAsMap().get("message"), equalTo("pod-" + i));
        }
    }

    public void testMatchByProvidedID() throws Exception {
        var indexName = "test-name";
        createIndex(indexName, Settings.builder().put("index.mode", "logsdb").build());
        Instant time = Instant.now();
        BulkRequest bulkRequest = new BulkRequest(indexName);
        int numDocs = randomIntBetween(16, 256);
        for (int j = 0; j < numDocs; j++) {
            var indexRequest = new IndexRequest(indexName).opType(DocWriteRequest.OpType.INDEX).id("id-" + j);
            indexRequest.source(
                DOC.replace("$time", formatInstant(time)).replace("$uuid", UUID.randomUUID().toString()).replace("$pod", "pod-" + j),
                XContentType.JSON
            );
            bulkRequest.add(indexRequest);
            time = time.plusMillis(1);
        }
        var bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.hasFailures(), is(false));
        client().admin().indices().refresh(new RefreshRequest(indexName)).actionGet();

        for (int i = 0; i < numDocs; i++) {
            String id = "id-" + i;
            var searchRequest = new SearchRequest(indexName);
            searchRequest.source(new SearchSourceBuilder().query(new TermQueryBuilder("_id", id)).size(10));
            var searchResponse = client().search(searchRequest).actionGet();
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) 1));
            assertThat(searchResponse.getHits().getHits()[0].getSourceAsMap().get("message"), equalTo("pod-" + i));
        }
    }

    public void testDeleteByProvidedID() throws Exception {
        var indexName = "test-name";
        createIndex(indexName, Settings.builder().put("index.mode", "logsdb").build());
        Instant time = Instant.now();
        BulkRequest bulkRequest = new BulkRequest(indexName);
        int numDocs = randomIntBetween(16, 256);
        for (int j = 0; j < numDocs; j++) {
            var indexRequest = new IndexRequest(indexName)
                .opType(DocWriteRequest.OpType.INDEX)
                .id("id-" + j);
            indexRequest.source(
                DOC.replace("$time", formatInstant(time))
                    .replace("$uuid", UUID.randomUUID().toString())
                    .replace("$pod", "pod-" + j),
                XContentType.JSON
            );
            bulkRequest.add(indexRequest);
            time = time.plusMillis(1);
        }
        var bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.hasFailures(), is(false));
        client().admin().indices().refresh(new RefreshRequest(indexName)).actionGet();

        if (randomBoolean()) {
            flush(indexName, randomBoolean());
        }


        var searchRequest = new SearchRequest(indexName);
        searchRequest.source().trackTotalHits(true);
        assertResponse(client().search(searchRequest), searchResponse -> {
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));
        });

        // test get with the provided IDs
        for (int i = 0; i < numDocs; i++) {
            DeleteResponse deleteResponse = client().prepareDelete(indexName, "id-" + i).get();
            assertThat(deleteResponse.status(), equalTo(RestStatus.OK));
        }

        assertThat(
            client().prepareSearch(indexName).get().getHits().getTotalHits().value(),
            equalTo(0)
        );
    }

    private void createTemplate(String dataStreamName) throws IOException {
        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("id");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName + "*"))
                .template(
                    new Template(
                        indexSettings(4, 0).put("index.mode", "logsdb").put("index.sort.field", "message,k8s.pod.uid,@timestamp").build(),
                        new CompressedXContent(MAPPING_TEMPLATE),
                        null
                    )
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet();
    }

    private void checkIndexSearchAndRetrieval(String dataStreamName, boolean routeOnSortFields) throws Exception {
        String[] uuis = {
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString() };
        int numBulkRequests = randomIntBetween(128, 1024);
        int numDocsPerBulk = randomIntBetween(16, 256);
        String indexName = null;
        {
            Instant time = Instant.now();
            for (int i = 0; i < numBulkRequests; i++) {
                BulkRequest bulkRequest = new BulkRequest(dataStreamName);
                for (int j = 0; j < numDocsPerBulk; j++) {
                    var indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
                    indexRequest.source(
                        DOC.replace("$time", formatInstant(time))
                            .replace("$uuid", uuis[j % uuis.length])
                            .replace("$pod", "pod-" + randomIntBetween(0, 10)),
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

        // Verify settings.
        final GetSettingsResponse getSettingsResponse = indicesAdmin().getSettings(
            new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(indexName).includeDefaults(false)
        ).actionGet();
        final Settings settings = getSettingsResponse.getIndexToSettings().get(indexName);
        assertEquals("message,k8s.pod.uid,@timestamp", settings.get("index.sort.field"));
        if (routeOnSortFields) {
            assertEquals("[message, k8s.pod.uid]", settings.get("index.routing_path"));
            assertEquals("true", settings.get("index.logsdb.route_on_sort_fields"));
        } else {
            assertNull(settings.get("index.routing_path"));
            assertNull(settings.get("index.logsdb.route_on_sort_fields"));
        }

        // Check the search api can synthesize _id
        final String idxName = indexName;
        var searchRequest = new SearchRequest(dataStreamName);
        searchRequest.source().trackTotalHits(true);
        assertResponse(client().search(searchRequest), searchResponse -> {
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numBulkRequests * numDocsPerBulk));

            for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
                String id = searchResponse.getHits().getHits()[i].getId();
                assertThat(id, notNullValue());

                // Check that the _id is gettable:
                var getResponse = client().get(new GetRequest(idxName).id(id)).actionGet();
                assertThat(getResponse.isExists(), is(true));
                assertThat(getResponse.getId(), equalTo(id));
            }
        });
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

    public void flush(String index, boolean force) throws IOException, ExecutionException, InterruptedException {
        logger.info("flushing index {} force={}", index, force);
        FlushRequest flushRequest = new FlushRequest(index).force(force);
        assertResponse(client().admin().indices().flush(flushRequest), response -> { assertEquals(0, response.getFailedShards()); });
    }

}
