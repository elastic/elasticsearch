/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.Build;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.elasticsearch.action.admin.indices.ResizeIndexTestUtils.resizeRequest;
import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
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

    /**
     * The provider only ever auto-selects logsdb_columnar in snapshot builds (see
     * {@code LogsdbIndexModeSettingsProvider}), so in a release build every value of
     * {@code cluster.logsdb_columnar.enabled} resolves to plain logsdb anyway; looping over both there would just
     * repeat an identical check.
     */
    private static List<Boolean> columnarEnabledValuesToTest() {
        return Build.current().isSnapshot() ? List.of(false, true) : List.of(false);
    }

    /**
     * Toggles the dynamic {@code cluster.logsdb_columnar.enabled} setting between {@code false} and {@code true} and
     * verifies the provider's auto-injection for both outcomes, rather than leaving it to a single random per-run
     * coin flip (the node, and any setting fixed once in {@link #nodeSettings()}, is reused across test methods).
     */
    public void testStandard() throws Exception {
        try {
            for (boolean columnarEnabled : columnarEnabledValuesToTest()) {
                updateClusterSettings(Settings.builder().put("cluster.logsdb_columnar.enabled", columnarEnabled).build());
                String dataStreamName = "logs-k8s-prod-" + columnarEnabled;
                var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("id-" + columnarEnabled);
                putTemplateRequest.indexTemplate(
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of(dataStreamName + "*"))
                        .template(
                            new Template(
                                indexSettings(4, 0).put("index.sort.field", "message,k8s.pod.uid,@timestamp").build(),
                                new CompressedXContent(MAPPING_TEMPLATE),
                                null
                            )
                        )
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                        .build()
                );
                client().execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet();
                checkIndexSearchAndRetrieval(dataStreamName, false, columnarEnabled);
            }
        } finally {
            updateClusterSettings(Settings.builder().putNull("cluster.logsdb_columnar.enabled").build());
        }
    }

    /**
     * See {@link #testStandard()} for why this deterministically exercises both {@code cluster.logsdb_columnar.enabled}
     * outcomes instead of relying on a random per-run coin flip.
     */
    public void testRouteOnSortFields() throws Exception {
        try {
            for (boolean columnarEnabled : columnarEnabledValuesToTest()) {
                updateClusterSettings(Settings.builder().put("cluster.logsdb_columnar.enabled", columnarEnabled).build());
                String dataStreamName = "logs-k8s-prod-" + columnarEnabled;
                var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("id-" + columnarEnabled);
                putTemplateRequest.indexTemplate(
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of(dataStreamName + "*"))
                        .template(
                            new Template(
                                indexSettings(4, 0).put("index.sort.field", "message,k8s.pod.uid,@timestamp")
                                    .put("index.logsdb.route_on_sort_fields", true)
                                    .build(),
                                new CompressedXContent(MAPPING_TEMPLATE),
                                null
                            )
                        )
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                        .build()
                );
                client().execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet();
                checkIndexSearchAndRetrieval(dataStreamName, true, columnarEnabled);
            }
        } finally {
            updateClusterSettings(Settings.builder().putNull("cluster.logsdb_columnar.enabled").build());
        }
    }

    private void checkIndexSearchAndRetrieval(String dataStreamName, boolean routeOnSortFields, boolean columnarEnabled) throws Exception {
        String[] uuis = {
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString() };
        // Kept modest because this now runs twice per test method (once per cluster.logsdb_columnar.enabled
        // value) against a fixed-size test JVM heap; the original wider range OOMs when doubled up.
        int numBulkRequests = randomIntBetween(32, 256);
        int numDocsPerBulk = randomIntBetween(8, 64);
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
        // The provider only auto-selects logsdb_columnar in snapshot builds; mirror that here so this
        // assertion holds in both snapshot and release-build test runs.
        boolean expectColumnar = columnarEnabled && Build.current().isSnapshot();
        assertEquals(expectColumnar ? "logsdb_columnar" : "logsdb", settings.get("index.mode"));
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

    public void testShrink() throws Exception {
        String indexMode = randomBoolean() ? "logsdb_columnar" : "logsdb";
        client().admin()
            .indices()
            .prepareCreate("my-logs")
            .setMapping("@timestamp", "type=date", "host.name", "type=keyword")
            .setSettings(indexSettings(between(3, 5), 0).put("index.mode", indexMode).put("index.sort.field", "host.name"))
            .get();

        long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-08-08T00:00:00Z");
        BulkRequest bulkRequest = new BulkRequest("my-logs");
        int numDocs = randomIntBetween(100, 10_000);
        for (int i = 0; i < numDocs; i++) {
            timestamp += randomIntBetween(0, 1000);
            String field = "field-" + randomIntBetween(1, 20);
            bulkRequest.add(
                new IndexRequest("my-logs").id(Integer.toString(i))
                    .source("host.name", "host-" + between(1, 5), "@timestamp", timestamp, field, randomNonNegativeLong())
            );
        }
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client().bulk(bulkRequest).actionGet();
        client().admin().indices().prepareFlush("my-logs").get();
        client().admin().indices().prepareUpdateSettings("my-logs").setSettings(Settings.builder().put("index.blocks.write", true)).get();

        client().execute(TransportResizeAction.TYPE, resizeRequest(ResizeType.SHRINK, "my-logs", "shrink-my-logs", indexSettings(1, 0)))
            .actionGet();
        assertNoFailures(client().admin().indices().prepareForceMerge("shrink-my-logs").setMaxNumSegments(1).setFlush(true).get());
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

}
