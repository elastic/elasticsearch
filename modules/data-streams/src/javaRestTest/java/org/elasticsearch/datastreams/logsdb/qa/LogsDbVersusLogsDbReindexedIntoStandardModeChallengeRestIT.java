/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.logsdb.qa;

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
 * This test compares behavior of a logsdb data stream and a standard index mode data stream
 * containing data reindexed from initial data stream.
 * There should be no differences between such two data streams.
 */
public class LogsDbVersusLogsDbReindexedIntoStandardModeChallengeRestIT extends ReindexChallengeRestIT {
    public String getBaselineDataStreamName() {
        return "logs-apache-baseline";
    }

    public String getContenderDataStreamName() {
        return "standard-apache-reindexed-contender";
    }

    @Override
    public void baselineSettings(Settings.Builder builder) {
        dataGenerationHelper.logsDbSettings(builder);
    }

    @Override
    public void contenderSettings(Settings.Builder builder) {

    }

    @Override
    public void baselineMappings(XContentBuilder builder) throws IOException {
        dataGenerationHelper.logsDbMapping(builder);
    }

    @Override
    public void contenderMappings(XContentBuilder builder) throws IOException {
        dataGenerationHelper.standardMapping(builder);
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
