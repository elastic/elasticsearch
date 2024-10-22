/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class LogsDbSourceModeMigrationIT extends LogsIndexModeRestTestIT {
    public static final String INDEX_TEMPLATE = """
        {
          "index_patterns": ["my-logs-*-*"],
          "priority": 100,
          "data_stream": {},
          "composed_of": [
            "my-logs-mapping",
            "my-logs-original-source",
            "my-logs-migrated-source"
          ],
          "ignore_missing_component_templates": ["my-logs-original-source", "my-logs-migrated-source"]
        }
        """;

    public static final String MAPPING_COMPONENT_TEMPLATE = """
        {
          "template": {
            "settings": {
              "index": {
                "mode": "logsdb"
              }
            },
            "mappings": {
              "properties": {
                "@timestamp": {
                  "type": "date",
                  "format": "epoch_millis"
                },
                "message": {
                  "type": "text"
                },
                "method": {
                  "type": "keyword"
                },
                "hits": {
                  "type": "long"
                }
              }
            }
          }
        }""";

    public static final String STORED_SOURCE_COMPONENT_TEMPLATE = """
        {
          "template": {
            "settings": {
              "index": {
                "mapping.source.mode": "stored"
              }
            }
          }
        }""";

    public static final String SYNTHETIC_SOURCE_COMPONENT_TEMPLATE = """
        {
          "template": {
            "settings": {
              "index": {
                "mapping.source.mode": "synthetic"
              }
            }
          }
        }""";

    @ClassRule()
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("constant-keyword")
        .module("data-streams")
        .module("mapper-extras")
        .module("x-pack-aggregate-metric")
        .module("x-pack-stack")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.otel_data.registry.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.logsdb.enabled", "true")
        .setting("stack.templates.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void setup() {
        client = client();
    }

    private RestClient client;

    public void testSwitchFromStoredToSyntheticSource() throws IOException {
        assertOK(putComponentTemplate(client, "my-logs-mapping", MAPPING_COMPONENT_TEMPLATE));
        assertOK(putComponentTemplate(client, "my-logs-original-source", STORED_SOURCE_COMPONENT_TEMPLATE));

        assertOK(putTemplate(client, "my-logs", INDEX_TEMPLATE));
        assertOK(createDataStream(client, "my-logs-ds-test"));

        var initialSourceMode = (String) getSetting(
            client,
            getDataStreamBackingIndex(client, "my-logs-ds-test", 0),
            "index.mapping.source.mode"
        );
        assertThat(initialSourceMode, equalTo("stored"));
        var initialIndexMode = (String) getSetting(client, getDataStreamBackingIndex(client, "my-logs-ds-test", 0), "index.mode");
        assertThat(initialIndexMode, equalTo("logsdb"));

        var indexedWithStoredSource = new ArrayList<XContentBuilder>();
        var indexedWithSyntheticSource = new ArrayList<XContentBuilder>();
        for (int i = 0; i < 10; i++) {
            indexedWithStoredSource.add(generateDoc());
            indexedWithSyntheticSource.add(generateDoc());
        }

        Response storedSourceBulkResponse = bulkIndex(client, "my-logs-ds-test", indexedWithStoredSource, 0);
        assertOK(storedSourceBulkResponse);
        assertThat(entityAsMap(storedSourceBulkResponse).get("errors"), Matchers.equalTo(false));

        assertOK(putComponentTemplate(client, "my-logs-migrated-source", SYNTHETIC_SOURCE_COMPONENT_TEMPLATE));
        var rolloverResponse = rolloverDataStream(client, "my-logs-ds-test");
        assertOK(rolloverResponse);
        assertThat(entityAsMap(rolloverResponse).get("rolled_over"), is(true));

        var finalSourceMode = (String) getSetting(
            client,
            getDataStreamBackingIndex(client, "my-logs-ds-test", 1),
            "index.mapping.source.mode"
        );
        assertThat(finalSourceMode, equalTo("synthetic"));

        Response syntheticSourceBulkResponse = bulkIndex(client, "my-logs-ds-test", indexedWithSyntheticSource, 10);
        assertOK(syntheticSourceBulkResponse);
        assertThat(entityAsMap(syntheticSourceBulkResponse).get("errors"), Matchers.equalTo(false));

        var allDocs = Stream.concat(indexedWithStoredSource.stream(), indexedWithSyntheticSource.stream()).toList();

        var sourceList = search(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(allDocs.size()), "my-logs-ds-test");
        assertThat(sourceList.size(), equalTo(allDocs.size()));

        for (int i = 0; i < sourceList.size(); i++) {
            var expected = XContentHelper.convertToMap(BytesReference.bytes(allDocs.get(i)), false, XContentType.JSON).v2();
            assertThat(sourceList.get(i), equalTo(expected));
        }
    }

    public void testSwitchFromSyntheticToStoredSource() throws IOException {
        assertOK(putComponentTemplate(client, "my-logs-mapping", MAPPING_COMPONENT_TEMPLATE));
        assertOK(putComponentTemplate(client, "my-logs-original-source", SYNTHETIC_SOURCE_COMPONENT_TEMPLATE));

        assertOK(putTemplate(client, "my-logs", INDEX_TEMPLATE));
        assertOK(createDataStream(client, "my-logs-ds-test"));

        var initialSourceMode = (String) getSetting(
            client,
            getDataStreamBackingIndex(client, "my-logs-ds-test", 0),
            "index.mapping.source.mode"
        );
        assertThat(initialSourceMode, equalTo("synthetic"));
        var initialIndexMode = (String) getSetting(client, getDataStreamBackingIndex(client, "my-logs-ds-test", 0), "index.mode");
        assertThat(initialIndexMode, equalTo("logsdb"));

        var indexedWithSyntheticSource = new ArrayList<XContentBuilder>();
        var indexedWithStoredSource = new ArrayList<XContentBuilder>();
        for (int i = 0; i < 10; i++) {
            indexedWithSyntheticSource.add(generateDoc());
            indexedWithStoredSource.add(generateDoc());
        }

        Response syntheticSourceBulkResponse = bulkIndex(client, "my-logs-ds-test", indexedWithSyntheticSource, 0);
        assertOK(syntheticSourceBulkResponse);
        assertThat(entityAsMap(syntheticSourceBulkResponse).get("errors"), Matchers.equalTo(false));

        assertOK(putComponentTemplate(client, "my-logs-migrated-source", STORED_SOURCE_COMPONENT_TEMPLATE));
        var rolloverResponse = rolloverDataStream(client, "my-logs-ds-test");
        assertOK(rolloverResponse);
        assertThat(entityAsMap(rolloverResponse).get("rolled_over"), is(true));

        var finalSourceMode = (String) getSetting(
            client,
            getDataStreamBackingIndex(client, "my-logs-ds-test", 1),
            "index.mapping.source.mode"
        );
        assertThat(finalSourceMode, equalTo("stored"));

        Response storedSourceBulkResponse = bulkIndex(client, "my-logs-ds-test", indexedWithStoredSource, 10);
        assertOK(storedSourceBulkResponse);
        assertThat(entityAsMap(storedSourceBulkResponse).get("errors"), Matchers.equalTo(false));

        var allDocs = Stream.concat(indexedWithSyntheticSource.stream(), indexedWithStoredSource.stream()).toList();

        var sourceList = search(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(allDocs.size()), "my-logs-ds-test");
        assertThat(sourceList.size(), equalTo(allDocs.size()));

        for (int i = 0; i < sourceList.size(); i++) {
            var expected = XContentHelper.convertToMap(BytesReference.bytes(allDocs.get(i)), false, XContentType.JSON).v2();
            assertThat(sourceList.get(i), equalTo(expected));
        }
    }

    private static Response bulkIndex(RestClient client, String dataStreamName, List<XContentBuilder> documents, int startId)
        throws IOException {
        var sb = new StringBuilder();
        int id = startId;
        for (var document : documents) {
            sb.append(Strings.format("{ \"create\": { \"_id\" : \"%d\" } }", id)).append("\n");
            sb.append(Strings.toString(document)).append("\n");
            id++;
        }

        var bulkRequest = new Request("POST", "/" + dataStreamName + "/_bulk");
        bulkRequest.setJsonEntity(sb.toString());
        bulkRequest.addParameter("refresh", "true");
        return client.performRequest(bulkRequest);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> search(SearchSourceBuilder search, String dataStreamName) throws IOException {
        var request = new Request("GET", "/" + dataStreamName + "/_search");
        request.setJsonEntity(Strings.toString(search));
        var searchResponse = client.performRequest(request);
        assertOK(searchResponse);

        Map<String, Object> searchResponseMap = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            searchResponse.getEntity().getContent(),
            false
        );
        var hitsMap = (Map<String, Object>) searchResponseMap.get("hits");

        var hitsList = (List<Map<String, Object>>) hitsMap.get("hits");
        assertThat(hitsList.size(), greaterThan(0));

        return hitsList.stream()
            .sorted(Comparator.comparingInt((Map<String, Object> hit) -> Integer.parseInt((String) hit.get("_id"))))
            .map(hit -> (Map<String, Object>) hit.get("_source"))
            .toList();
    }

    private static XContentBuilder generateDoc() throws IOException {
        var doc = XContentFactory.jsonBuilder();
        doc.startObject();
        {
            doc.field("@timestamp", Long.toString(randomMillisUpToYear9999()));
            doc.field("message", randomAlphaOfLengthBetween(20, 50));
            doc.field("method", randomAlphaOfLength(3));
            doc.field("hits", randomLong());
        }
        doc.endObject();

        return doc;
    }
}
