/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class LogsIndexModeCustomSettingsIT extends LogsIndexModeRestTestIT {
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

    @SuppressWarnings("unchecked")
    public void testConfigureStoredSource() throws IOException {
        var storedSourceMapping = """
            {
              "template": {
                "mappings": {
                  "_source": {
                    "mode": "stored"
                  }
                }
              }
            }""";

        Exception e = assertThrows(ResponseException.class, () -> putComponentTemplate(client, "logs@custom", storedSourceMapping));
        assertThat(
            e.getMessage(),
            containsString("updating component template [logs@custom] results in invalid composable template [logs]")
        );
        assertThat(e.getMessage(), containsString("indices with [index.mode=logs] only support synthetic source"));

        assertOK(createDataStream(client, "logs-custom-dev"));

        var mapping = getMapping(client, getDataStreamBackingIndex(client, "logs-custom-dev", 0));
        String sourceMode = ((Map<String, String>) mapping.get("_source")).get("mode");
        assertThat(sourceMode, equalTo("synthetic"));
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> getMapping(final RestClient client, final String indexName) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_mapping");
        final Map<String, Object> mappings = ((Map<String, Map<String, Object>>) entityAsMap(client.performRequest(request)).get(indexName))
            .get("mappings");

        return mappings;
    }
}
