/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

/**
 * This test compares behavior of a logsdb data stream and a data stream containing
 * data reindexed from initial data stream.
 * There should be no differences between such two data streams.
 */
public class LogsDbVersusReindexedLogsDbChallengeRestIT extends ReindexChallengeRestIT {
    public String getBaselineDataStreamName() {
        return "logs-apache-baseline";
    }

    public String getContenderDataStreamName() {
        return "logs-apache-reindexed-contender";
    }

    @Override
    public void baselineSettings(Settings.Builder builder) {
        dataGenerationHelper.logsDbSettings(builder);
    }

    @Override
    public void contenderSettings(Settings.Builder builder) {
        dataGenerationHelper.logsDbSettings(builder);
    }

    @Override
    public void baselineMappings(XContentBuilder builder) throws IOException {
        dataGenerationHelper.logsDbMapping(builder);
    }

    @Override
    public void contenderMappings(XContentBuilder builder) throws IOException {
        dataGenerationHelper.logsDbMapping(builder);
    }

    @Override
    public Response indexContenderDocuments(CheckedSupplier<List<XContentBuilder>, IOException> documentsSupplier) throws IOException {
        var reindexRequest = new Request("POST", "/_reindex?refresh=true");
        reindexRequest.setJsonEntity(String.format(Locale.ROOT, """
            {
                "source": {
                    "index": "%s"
                },
                "dest": {
                  "index": "%s",
                  "op_type": "create"
                }
            }
            """, getBaselineDataStreamName(), getContenderDataStreamName()));
        var response = client.performRequest(reindexRequest);
        assertOK(response);

        var body = entityAsMap(response);
        assertThat("encountered failures when performing reindex:\n " + body, body.get("failures"), equalTo(List.of()));

        return response;
    }
}
