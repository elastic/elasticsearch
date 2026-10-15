/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.nullValue;

/**
 * Bulk requests that map new fields with {@code index.mapping.combine_dynamic_updates}, while the nodes are upgraded one by one.
 * The primaries of the index move between upgraded and not upgraded nodes, and the master is either.
 */
public class CombineDynamicMappingUpdatesRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    private static final String SETTING = "index.mapping.combine_dynamic_updates";
    private static final String FEATURE = "mapper.combine_dynamic_updates";
    private static final DateFormatter FORMATTER = DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName());

    private final int upgradedNodes;

    public CombineDynamicMappingUpdatesRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
        this.upgradedNodes = upgradedNodes;
    }

    /**
     * The setting is enabled as soon as every node supports it: on the old cluster if it already does, otherwise once all the
     * nodes are upgraded. Every bulk request maps a new field per document, and fields that the other requests map too.
     */
    public void testBulkRequestsMapNewFields() throws IOException {
        final String index = "combine-dynamic-updates";
        if (isOldCluster()) {
            final Request createIndex = new Request("PUT", "/" + index);
            createIndex.setJsonEntity(Strings.format("""
                {
                  "settings": {
                    "number_of_shards": 3,
                    "number_of_replicas": 1,
                    "mapping.total_fields.limit": 10000%s
                  }
                }""", oldClusterHasFeature(FEATURE) ? ", \"mapping.combine_dynamic_updates\": true" : ""));
            client().performRequest(createIndex);
        } else if (isUpgradedCluster() && oldClusterHasFeature(FEATURE) == false) {
            final Request updateSettings = new Request("PUT", "/" + index + "/_settings");
            updateSettings.setJsonEntity("{\"" + SETTING + "\": true}");
            client().performRequest(updateSettings);
        }
        final String prefix = isOldCluster() ? "old" : isMixedCluster() ? "mixed_" + upgradedNodes : "upgraded";
        final int numDocs = randomIntBetween(10, 200);
        final StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append(Strings.format("{\"%s_%d\": %d, \"shared_%d\": \"value\"}\n", prefix, i, i, i % 10));
        }
        final Request bulkRequest = new Request("POST", "/" + index + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        assertNoBulkErrors(client().performRequest(bulkRequest));

        final Map<String, Object> properties = mappingProperties(index);
        for (int i = 0; i < numDocs; i++) {
            assertThat(properties, hasKey(prefix + "_" + i));
            assertThat(ObjectPath.evaluate(properties, prefix + "_" + i + ".type"), equalTo("long"));
        }
        for (int i = 0; i < Math.min(numDocs, 10); i++) {
            assertThat(ObjectPath.evaluate(properties, "shared_" + i + ".type"), equalTo("text"));
        }
        final Request count = new Request("GET", "/" + index + "/_count");
        count.addParameter("q", prefix + "_0:0");
        assertThat(ObjectPath.createFromResponse(client().performRequest(count)).evaluate("count"), equalTo(1));
        if (isUpgradedCluster()) {
            assertThat(indexSetting(index, SETTING), equalTo("true"));
        }
    }

    /**
     * The backing indices of time series data streams get the setting once every node supports it.
     */
    public void testTimeSeriesDataStreamDefault() throws IOException {
        if (isOldCluster()) {
            return;
        }
        final String dataStream = "tsdb-combine-" + (isMixedCluster() ? "mixed-" + upgradedNodes : "upgraded");
        final Request template = new Request("PUT", "/_index_template/" + dataStream);
        template.setJsonEntity(Strings.format("""
            {
              "index_patterns": ["%s"],
              "data_stream": {},
              "template": {
                "settings": { "index.mode": "time_series", "index.routing_path": ["host"] },
                "mappings": {
                  "properties": {
                    "@timestamp": { "type": "date" },
                    "host": { "type": "keyword", "time_series_dimension": true }
                  }
                }
              }
            }""", dataStream));
        client().performRequest(template);

        final StringBuilder bulk = new StringBuilder();
        final int numDocs = randomIntBetween(10, 100);
        final String timestamp = FORMATTER.format(Instant.now());
        for (int i = 0; i < numDocs; i++) {
            bulk.append("{\"create\":{}}\n");
            bulk.append(Strings.format("{\"@timestamp\": \"%s\", \"host\": \"host-%d\", \"metric_%d\": %d}\n", timestamp, i, i, i));
        }
        final Request bulkRequest = new Request("POST", "/" + dataStream + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        assertNoBulkErrors(client().performRequest(bulkRequest));

        final Response getDataStream = client().performRequest(new Request("GET", "/_data_stream/" + dataStream));
        final String backingIndex = ObjectPath.createFromResponse(getDataStream).evaluate("data_streams.0.indices.0.index_name");
        final Map<String, Object> properties = mappingProperties(backingIndex);
        for (int i = 0; i < numDocs; i++) {
            assertThat(properties, hasKey("metric_" + i));
        }
        // a mixed cluster creates indices with the index version of the old nodes
        final boolean expected = isUpgradedCluster()
            || getOldClusterIndexVersion().onOrAfter(IndexVersions.TIME_SERIES_COMBINE_DYNAMIC_UPDATES_DEFAULT);
        assertThat(indexSetting(backingIndex, SETTING), expected ? equalTo("true") : nullValue());
    }

    private static void assertNoBulkErrors(Response response) throws IOException {
        final ObjectPath bulkResponse = ObjectPath.createFromResponse(response);
        if (Boolean.TRUE.equals(bulkResponse.evaluate("errors"))) {
            final Object items = bulkResponse.evaluate("items");
            fail("bulk request failed: " + items);
        }
    }

    // the names of backing indices have dots, which paths can't address
    private static Map<String, Object> mappingProperties(String index) throws IOException {
        final Map<String, Object> response = entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_mapping")));
        return ObjectPath.evaluate(response.get(index), "mappings.properties");
    }

    private static Object indexSetting(String index, String setting) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("flat_settings", "true");
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        final Map<String, Object> settings = ObjectPath.evaluate(response.get(index), "settings");
        return settings.get(setting);
    }
}
