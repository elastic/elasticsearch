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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

public abstract class ReindexChallengeRestIT extends StandardVersusLogsIndexModeRandomDataChallengeRestIT {
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
