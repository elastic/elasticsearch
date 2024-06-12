/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class LogsIndexModeEnabledRestTestIT extends LogsIndexModeRestTestIT {

    @ClassRule()
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("constant-keyword")
        .module("data-streams")
        .module("mapper-extras")
        .module("x-pack-aggregate-metric")
        .module("x-pack-stack")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.logsdb.enabled", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void setup() throws Exception {
        client = client();
        waitForLogs(client);
    }

    private RestClient client;

    private static final String MAPPINGS = """
        {
          "template": {
            "mappings": {
              "properties": {
                "method": {
                  "type": "keyword"
                },
                "message": {
                  "type": "text"
                }
              }
            }
          }
        }""";

    private static String BULK_INDEX_REQUEST = """
        { "create": {}}
        { "@timestamp": "2023-01-01T05:11:00Z", "host.name": "foo", "method" : "PUT", "message": "foo put message" }
        { "create": {}}
        { "@timestamp": "2023-01-01T05:12:00Z", "host.name": "bar", "method" : "POST", "message": "bar post message" }
        { "create": {}}
        { "@timestamp": "2023-01-01T05:12:00Z", "host.name": "baz", "method" : "PUT", "message": "baz put message" }
        { "create": {}}
        { "@timestamp": "2023-01-01T05:13:00Z", "host.name": "baz", "method" : "PUT", "message": "baz put message" }
        """;

    public void testCreateDataStream() throws IOException {
        assertOK(putComponentTemplate(client, "logs@custom", MAPPINGS));
        assertOK(createDataStream(client, "logs-custom-dev"));
        final String indexMode = (String) getSetting(client, getDataStreamBackingIndex(client, "logs-custom-dev", 0), "index.mode");
        assertThat(indexMode, equalTo(IndexMode.LOGS.getName()));
    }

    public void testBulkIndexing() throws IOException {
        assertOK(putComponentTemplate(client, "logs@custom", MAPPINGS));
        assertOK(createDataStream(client, "logs-custom-dev"));
        assertOK(bulkIndex(client, "logs-custom-dev", () -> BULK_INDEX_REQUEST));
    }

    public void testRolloverDataStream() throws IOException {
        assertOK(putComponentTemplate(client, "logs@custom", MAPPINGS));
        assertOK(createDataStream(client, "logs-custom-dev"));
        final String firstBackingIndex = getDataStreamBackingIndex(client, "logs-custom-dev", 0);
        assertOK(rolloverDataStream(client, "logs-custom-dev"));
        final String secondBackingIndex = getDataStreamBackingIndex(client, "logs-custom-dev", 1);
        assertThat(firstBackingIndex, Matchers.not(equalTo(secondBackingIndex)));
        assertThat(getDataStreamBackingIndices(client, "logs-custom-dev").size(), equalTo(2));
    }
}
