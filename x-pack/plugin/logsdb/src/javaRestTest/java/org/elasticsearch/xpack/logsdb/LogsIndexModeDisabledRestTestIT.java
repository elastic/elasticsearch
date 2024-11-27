/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class LogsIndexModeDisabledRestTestIT extends LogsIndexModeRestTestIT {

    private static final String MAPPINGS = """
        {
          "template": {
            "mappings": {
              "properties": {
                "@timestamp": {
                  "type": "date"
                },
                "message": {
                  "type": "text"
                }
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
        .setting("xpack.license.self_generated.type", "trial")
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

    public void testLogsSettingsIndexModeDisabled() throws IOException {
        assertOK(createDataStream(client, "logs-custom-dev"));
        final String indexMode = (String) getSetting(
            client,
            getDataStreamBackingIndex(client, "logs-custom-dev", 0),
            IndexSettings.MODE.getKey()
        );
        assertThat(indexMode, Matchers.not(equalTo(IndexMode.LOGSDB.getName())));
    }

    public void testTogglingLogsdb() throws IOException {
        putComponentTemplate(client, "logs@settings", MAPPINGS);
        assertOK(createDataStream(client, "logs-custom-dev"));
        final String indexModeBefore = (String) getSetting(
            client,
            getDataStreamBackingIndex(client, "logs-custom-dev", 0),
            IndexSettings.MODE.getKey()
        );
        assertThat(indexModeBefore, Matchers.not(equalTo(IndexMode.LOGSDB.getName())));
        assertOK(putClusterSetting(client, "cluster.logsdb.enabled", "true"));
        final String indexModeAfter = (String) getSetting(
            client,
            getDataStreamBackingIndex(client, "logs-custom-dev", 0),
            IndexSettings.MODE.getKey()
        );
        assertThat(indexModeAfter, Matchers.not(equalTo(IndexMode.LOGSDB.getName())));
        assertOK(rolloverDataStream(client, "logs-custom-dev"));
        final String indexModeLater = (String) getSetting(
            client,
            getDataStreamBackingIndex(client, "logs-custom-dev", 1),
            IndexSettings.MODE.getKey()
        );
        assertThat(indexModeLater, equalTo(IndexMode.LOGSDB.getName()));
        assertOK(putClusterSetting(client, "cluster.logsdb.enabled", "false"));
        assertOK(rolloverDataStream(client, "logs-custom-dev"));
        final String indexModeFinal = (String) getSetting(
            client,
            getDataStreamBackingIndex(client, "logs-custom-dev", 2),
            IndexSettings.MODE.getKey()
        );
        assertThat(indexModeFinal, Matchers.not(equalTo(IndexMode.LOGSDB.getName())));

    }

    public void testEnablingLogsdb() throws IOException {
        putComponentTemplate(client, "logs@settings", MAPPINGS);
        assertOK(putClusterSetting(client, "cluster.logsdb.enabled", true));
        assertOK(createDataStream(client, "logs-custom-dev"));
        final String indexMode = (String) getSetting(
            client,
            getDataStreamBackingIndex(client, "logs-custom-dev", 0),
            IndexSettings.MODE.getKey()
        );
        assertThat(indexMode, equalTo(IndexMode.LOGSDB.getName()));
        assertOK(putClusterSetting(client, "cluster.logsdb.enabled", false));
    }

}
