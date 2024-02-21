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
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentType;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TSDBPassthroughIndexingIT extends ESSingleNodeTestCase {

    public static final String MAPPING_TEMPLATE = """
        {
          "_doc":{
            "properties": {
              "@timestamp" : {
                "type": "date"
              },
              "attributes": {
                "type": "passthrough",
                "dynamic": true,
                "time_series_dimension": true
              },
              "metrics": {
                "properties": {
                    "network": {
                        "properties": {
                            "tx": {
                                "type": "long",
                                "time_series_metric": "counter"
                            },
                            "rx": {
                                "type": "long",
                                "time_series_metric": "counter"
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
            "attributes": {
                "metricset": "pod",
                "pod": {
                    "name": "$name",
                    "uid": "$uid",
                    "ip": "$ip"
                }
            },
            "metrics": {
                "network": {
                    "tx": 1434595272,
                    "rx": 530605511
                }
            }
        }
        """;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DataStreamsPlugin.class, InternalSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings());
        // This essentially disables the automatic updates to end_time settings of a data stream's latest backing index.
        newSettings.put(DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL.getKey(), "10m");
        return newSettings.build();
    }

    public void testIndexingGettingAndSearching() throws Exception {
        var templateSettings = Settings.builder()
            .put("index.mode", "time_series")
            .put("index.number_of_shards", randomIntBetween(2, 10))
            .put("index.number_of_replicas", 0);

        var request = new TransportPutComposableIndexTemplateAction.Request("id");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("k8s*"))
                .template(new Template(templateSettings.build(), new CompressedXContent(MAPPING_TEMPLATE), null))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();

        String index = null;
        int indexingIters = randomIntBetween(16, 128);
        Instant time = Instant.now();
        for (int i = 0; i < indexingIters; i++) {
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(
                DOC.replace("$time", formatInstant(time))
                    .replace("$uid", randomUUID())
                    .replace("$name", randomAlphaOfLength(4))
                    .replace("$ip", InetAddresses.toAddrString(randomIp(randomBoolean()))),
                XContentType.JSON
            );
            var indexResponse = client().index(indexRequest).actionGet();
            index = indexResponse.getIndex();
            String id = indexResponse.getId();

            var getResponse = client().get(new GetRequest(index, id)).actionGet();
            assertThat(getResponse.isExists(), is(true));

            client().admin().indices().refresh(new RefreshRequest(index)).actionGet();
            var searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().query(new TermQueryBuilder("_id", id)));
            assertResponse(client().search(searchRequest), searchResponse -> {
                assertHitCount(searchResponse, 1);
                assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo(id));
            });
            var deleteResponse = client().delete(new DeleteRequest(index, id)).actionGet();
            assertThat(deleteResponse.getIndex(), equalTo(index));
            assertThat(deleteResponse.getId(), equalTo(id));
            assertThat(deleteResponse.getResult(), equalTo(DocWriteResponse.Result.DELETED));
            time = time.plusMillis(1);
        }

        // validate index:
        var getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(index)).actionGet();
        assertThat(getIndexResponse.getSettings().get(index).get("index.routing_path"), equalTo("[attributes.*]"));
        // validate mapping
        var mapping = getIndexResponse.mappings().get(index).getSourceAsMap();
        assertMap(
            ObjectPath.eval("properties.attributes.properties.metricset", mapping),
            matchesMap().entry("type", "keyword").entry("time_series_dimension", true)
        );
        @SuppressWarnings("unchecked")
        var attributes = (Map<String, Map<?, ?>>) ObjectPath.eval("properties.attributes.properties", mapping);
        assertMap(attributes.get("pod.ip"), matchesMap().entry("type", "keyword").entry("time_series_dimension", true));
        assertMap(attributes.get("pod.uid"), matchesMap().entry("type", "keyword").entry("time_series_dimension", true));
        assertMap(attributes.get("pod.name"), matchesMap().entry("type", "keyword").entry("time_series_dimension", true));
        // alias field mappers:
        assertMap(
            ObjectPath.eval("properties.metricset", mapping),
            matchesMap().entry("type", "alias").entry("path", "attributes.metricset")
        );
        assertMap(
            ObjectPath.eval("properties.pod.properties", mapping),
            matchesMap().extraOk().entry("name", matchesMap().entry("type", "alias").entry("path", "attributes.pod.name"))
        );
        assertMap(
            ObjectPath.eval("properties.pod.properties", mapping),
            matchesMap().extraOk().entry("uid", matchesMap().entry("type", "alias").entry("path", "attributes.pod.uid"))
        );
        assertMap(
            ObjectPath.eval("properties.pod.properties", mapping),
            matchesMap().extraOk().entry("ip", matchesMap().entry("type", "alias").entry("path", "attributes.pod.ip"))
        );
    }

    public void testIndexingGettingAndSearchingShrunkIndex() throws Exception {
        String dataStreamName = "k8s";
        var templateSettings = Settings.builder()
            .put("index.mode", "time_series")
            .put("index.number_of_shards", 8)
            .put("index.number_of_replicas", 0);

        var request = new TransportPutComposableIndexTemplateAction.Request("id");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("k8s*"))
                .template(new Template(templateSettings.build(), new CompressedXContent(MAPPING_TEMPLATE), null))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();

        Instant time = Instant.now();
        int numBulkItems = randomIntBetween(16, 128);
        var bulkRequest = new BulkRequest(dataStreamName);
        for (int i = 0; i < numBulkItems; i++) {
            var indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(
                DOC.replace("$time", formatInstant(time))
                    .replace("$uid", randomUUID())
                    .replace("$name", randomAlphaOfLength(4))
                    .replace("$ip", InetAddresses.toAddrString(randomIp(randomBoolean()))),
                XContentType.JSON
            );
            bulkRequest.add(indexRequest);
            time = time.plusMillis(1);
        }

        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        var bulkResponse = client().bulk(bulkRequest).actionGet();
        for (var itemResponse : bulkResponse) {
            String id = itemResponse.getId();
            String index = itemResponse.getIndex();
            var getResponse = client().get(new GetRequest(index, id)).actionGet();
            assertThat(getResponse.isExists(), is(true));

            var searchRequest = new SearchRequest(index);
            searchRequest.source(new SearchSourceBuilder().query(new TermQueryBuilder("_id", id)));
            assertResponse(client().search(searchRequest), searchResponse -> {
                assertHitCount(searchResponse, 1);
                assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo(id));
            });
        }

        var rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet();
        assertThat(rolloverResponse.isRolledOver(), is(true));
        String sourceIndex = rolloverResponse.getOldIndex();

        var updateSettingsResponse = client().admin()
            .indices()
            .updateSettings(new UpdateSettingsRequest(sourceIndex).settings(Settings.builder().put("index.blocks.write", true)))
            .actionGet();
        assertThat(updateSettingsResponse.isAcknowledged(), is(true));

        String shrunkenTarget = "k8s-shrunken";
        var shrinkIndexResponse = client().admin()
            .indices()
            .prepareResizeIndex(sourceIndex, shrunkenTarget)
            .setResizeType(ResizeType.SHRINK)
            .setSettings(indexSettings(2, 0).build())
            .get();
        assertThat(shrinkIndexResponse.isAcknowledged(), is(true));
        assertThat(shrinkIndexResponse.index(), equalTo(shrunkenTarget));

        for (var itemResponse : bulkResponse) {
            String id = itemResponse.getId();
            var getResponse = client().get(new GetRequest(shrunkenTarget, id)).actionGet();
            assertThat(getResponse.isExists(), is(true));

            var searchRequest = new SearchRequest(shrunkenTarget);
            searchRequest.source(new SearchSourceBuilder().query(new TermQueryBuilder("_id", id)));
            assertResponse(client().search(searchRequest), searchResponse -> {
                assertHitCount(searchResponse, 1);
                assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo(id));
            });
        }
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

}
